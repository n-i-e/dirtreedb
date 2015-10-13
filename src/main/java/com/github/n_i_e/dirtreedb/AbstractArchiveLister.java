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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public abstract class AbstractArchiveLister implements IArchiveLister {
	protected PathEntry basepath;
	protected PathEntry next_entry;
	
	AbstractArchiveLister(PathEntry basepath) {
		this.basepath = basepath;
		next_entry = null;
	}

	protected abstract void getNext(boolean csum) throws IOException;
	
	public boolean hasNext(boolean csum) throws IOException {
		getNext(csum);
		if (next_entry == null) {
			return false;
		} else {
			return true;
		}
	}

	public PathEntry next(boolean csum) throws IOException {
		getNext(csum);

		PathEntry result = next_entry;
		next_entry = null;
		return result;
	}

	public boolean hasNext() throws IOException {
		return hasNext(false);
	}

	public PathEntry next() throws IOException {
		return next(false);
	}

	public InputStream getInputStream(PathEntry entry) throws IOException {
		while (hasNext(false)) {
			PathEntry p = next(false);
			if (p.getPath().equals(entry.getPath())) {
				return new BufferedInputStream(getInputStream());
				// BufferedInputStream here, because I want markSupported() stream at ApacheCompressArchiveLister constructor.
			}
		}
		throw new FileNotFoundException(String.format("!! Archive file not found for path %s at basepath %s",
				entry.getPath(), basepath.getPath()));
	}

	public void close() throws IOException {
		getInputStream().close();
	}

}
