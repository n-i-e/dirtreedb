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

package com.github.n_i_e.dirtreedb.lister;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.github.n_i_e.dirtreedb.PathEntry;

public abstract class PathEntryLister implements Iterator<PathEntry>, Iterable<PathEntry>, Closeable {

	private PathEntry basePath;

	protected PathEntry getBasePath() {
		return basePath;
	}
	protected void setBasePath(PathEntry basepath) {
		this.basePath = basepath;
	}
	private boolean csumRequested = false;

	protected boolean isCsumRequested() {
		return csumRequested;
	}
	public void setCsumRequested(boolean csumNow) {
		this.csumRequested = csumNow;
	}

	public PathEntryLister(PathEntry basepath) {
		setBasePath(basepath);
		setCsumRequested(false);
	}

	private IOException exceptionCache = null;

	protected IOException getExceptionCache() {
		return exceptionCache;
	}
	protected void setExceptionCache(IOException exceptionCache) {
		this.exceptionCache = exceptionCache;
	}

	@Override
	public void close() throws IOException {
		if (getExceptionCache() != null) {
			throw getExceptionCache();
		}
	}

	public abstract InputStream getInputStream() throws IOException;
	public abstract InputStream getInputStream(PathEntry entry) throws IOException;
}
