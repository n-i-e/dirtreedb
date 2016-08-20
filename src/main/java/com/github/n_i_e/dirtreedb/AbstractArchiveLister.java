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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public abstract class AbstractArchiveLister extends PathEntryLister {

	private PathEntry next_entry;

	public AbstractArchiveLister(PathEntry basepath) {
		super(basepath);
		Assertion.assertNullPointerException(basepath != null);
		Assertion.assertAssertionError(basepath.isFile() || basepath.isCompressedFile());
		next_entry = null;
	}

	protected abstract PathEntry getNext() throws IOException;

	private Set<String> pathnameUniquenessChecker = new HashSet<String> ();
	private void getNextWithIntegrityCheck() throws IOException {
		if (next_entry == null) {
			next_entry = getNext();
			if (next_entry != null) {
				Assertion.assertIOException(!pathnameUniquenessChecker.contains(next_entry.getPath()),
						"!! duplicate pathname: " + next_entry.getPath()
						);
				pathnameUniquenessChecker.add(next_entry.getPath());
			}
		}
	}

	@Override
	public boolean hasNext() {
		if (getExceptionCache() != null) { return false; }
		try {
			getNextWithIntegrityCheck();
		} catch (IOException e) {
			setExceptionCache(e);
			return false;
		}
		if (next_entry == null) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public PathEntry next() {
		if (getExceptionCache() != null) { return null; }
		try {
			getNextWithIntegrityCheck();
		} catch (IOException e) {
			setExceptionCache(e);
			return null;
		}
		PathEntry result = next_entry;
		next_entry = null;
		return result;
	}

	@Override
	public InputStream getInputStream(PathEntry entry) throws IOException {
		while (hasNext()) {
			PathEntry p = next();
			if (p.getPath().equals(entry.getPath())) {
				return new BufferedInputStream(getInputStream());
				// BufferedInputStream here, because I want markSupported() stream at ApacheCompressArchiveLister constructor.
			}
		}
		throw new FileNotFoundException(String.format("!! Archive file not found for path %s at basepath %s",
				entry.getPath(), getBasePath().getPath()));
	}

	@Override
	public Iterator<PathEntry> iterator() {
		return this;
	}

}
