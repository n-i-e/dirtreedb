/*
 * Copyright 2015 Namihiko Matsumura (https://github.com/n-i-e/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.n_i_e.dirtreedb;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public abstract class AbstractCompressorLister implements IArchiveLister {
	protected PathEntry next_entry;
	protected InputStream instream;

	AbstractCompressorLister(PathEntry basepath, InputStream inf) {
		String p = getBasename(basepath);

		next_entry = new PathEntry(basepath.getPath() + "/" + p, PathEntry.COMPRESSEDFILE);

		next_entry.setDateLastModified(basepath.getDateLastModified());
		next_entry.setCompressedSize(basepath.getSize());
		next_entry.setStatus(PathEntry.DIRTY);
	}

	public static String getBasename(PathEntry basepath) {
		int i1 = basepath.getPath().lastIndexOf("\\")+1;
		int i2 = basepath.getPath().lastIndexOf("/")+1;
		if (i2>i1) {
			i1 = i2;
		}
		int i3 = basepath.getPath().lastIndexOf(".");

		String p;
		if (i1 >= 0 && i3 >= 0) {
			p = basepath.getPath().substring(i1, i3);
		} else if (i1 >= 0 && i3 == -1) {
			p = basepath.getPath().substring(i1);
		} else if (i1 == -1 && i3 >= 0) {
			p = basepath.getPath().substring(0, i3);
		} else {
			p = basepath.getPath();
		}
		return p;
	}

	public boolean hasNext(boolean csum) {
		if (next_entry == null) {
			return false;
		} else {
			return true;
		}
	}

	public PathEntry next(boolean csum) throws IOException {
		if (next_entry == null) {
			return null;
		} else {
			if (csum) {
				next_entry.setCsum(instream);
			}
			PathEntry result = next_entry;
			next_entry = null;
			return result;
		}
	}

	public boolean hasNext() {
		return hasNext(false);
	}

	public PathEntry next() throws IOException {
		return next(false);
	}

	public InputStream getInputStream(PathEntry entry) throws IOException {
		if (next_entry.getPath().equals(entry.getPath())) {
			return new BufferedInputStream(instream); // BufferedInputStream is needed here, because I want markSupported() stream at ApacheCompressArchiveLister constructor.
		} else {
			throw new IOException("Compressed path do not match: " + entry.getPath() + " != " + next_entry.getPath());
		}
	}

	public InputStream getInputStream() {
		return new BufferedInputStream(instream);
	}

	public void close() throws IOException {
		instream.close();
	}
}
