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
import java.util.Iterator;

public abstract class AbstractCompressorLister extends PathEntryLister {
	private PathEntry next_entry;

	private InputStream instream = null;

	protected InputStream getInstream() {
		return instream;
	}

	protected void setInstream(InputStream instream) {
		Assertion.assertNullPointerException(instream != null);
		this.instream = instream;

		if (isCsumRequested()) {
			try {
				next_entry.setCsum(getInputStream());
			} catch (IOException e) {
				setExceptionCache(e);
			}
		}
	}

	public AbstractCompressorLister(PathEntry basepath, InputStream inf) {
		super(basepath);
		Assertion.assertNullPointerException(basepath != null);
		Assertion.assertNullPointerException(inf != null);
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

	@Override
	public boolean hasNext() {
		if (getExceptionCache() != null || next_entry == null || getInstream() == null) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public PathEntry next() {
		if (getExceptionCache() != null || next_entry == null || getInstream() == null) {
			return null;
		} else {
			PathEntry result = next_entry;
			next_entry = null;
			return result;
		}
	}

	@Override
	public InputStream getInputStream(PathEntry entry) throws IOException {
		if (next_entry.getPath().equals(entry.getPath())) {
			return new BufferedInputStream(instream); // BufferedInputStream is needed here, because I want markSupported() stream at ApacheCompressArchiveLister constructor.
		} else {
			throw new IOException("Compressed path do not match: " + entry.getPath() + " != " + next_entry.getPath());
		}
	}

	@Override
	public InputStream getInputStream() {
		return new BufferedInputStream(instream);
	}

	@Override
	public void close() throws IOException {
		super.close();
		if (getInputStream() != null) {
			getInputStream().close();
		}
	}

	@Override
	public Iterator<PathEntry> iterator() {
		return this;
	}

}
