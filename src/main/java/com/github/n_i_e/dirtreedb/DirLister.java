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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirLister implements IDirArchiveLister {
	DbPathEntry basepath;
	Iterator<String> file_iter;
	PathEntry next_entry;

	DirLister(DbPathEntry entry, File fileobj) throws FileNotFoundException {
		basepath = entry;
		String[] s = fileobj.list();
		if (s == null) {
			throw new FileNotFoundException("!! Folder not accessible");
		}
		file_iter = Arrays.asList(s).iterator();
		next_entry = null;
	}

	DirLister(DbPathEntry entry) throws FileNotFoundException {
		this(entry, new File(entry.getPath()));
	}

	private void getNext() throws IOException {
		while (next_entry == null && file_iter.hasNext()) {
			String n = file_iter.next();
			Assertion.assertAssertionError(n.length() > 0);
			Assertion.assertAssertionError(! ".".equals(n));
			Assertion.assertAssertionError(! "..".equals(n));
			String p = basepath.getPath() + n;
			next_entry = new PathEntry(new File(p));
			if (next_entry != null && next_entry.getSize() < 0) { // size is sometimes <0; JavaVM bug?
				next_entry.setCsumAndClose(next_entry.getInputStream());
			}
		}
	}

	private Set<String> pathnameUniquenessChecker = new HashSet<String> ();
	private void getNextWithIntegrityCheck() throws IOException {
		if (next_entry != null) {
			getNext();
		} else {
			getNext();
			if (next_entry != null) {
				Assertion.assertIOException(!pathnameUniquenessChecker.contains(next_entry.getPath()),
						"!! duplicate pathname: " + next_entry.getPath()
						);
				pathnameUniquenessChecker.add(next_entry.getPath());
			}
		}
	}

	public boolean hasNext() throws IOException {
		getNextWithIntegrityCheck();
		if (next_entry == null) {
			return false;
		} else {
			return true;
		}
	}

	public PathEntry next() throws IOException {
		getNextWithIntegrityCheck();

		PathEntry result = next_entry;
		next_entry = null;
		return result;
	}

	public InputStream getInputStream(PathEntry entry) throws IOException {
		return new BufferedInputStream(new FileInputStream(new File(entry.getPath())), 1*1024*1024);
	}

	public InputStream getInputStream() {
		return null;
	}

	public void close() throws IOException {
	}
}
