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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirLister extends PathEntryLister {
	private Iterator<File> file_iter;
	private PathEntry current_entry = null;
	private PathEntry next_entry = null;

	public DirLister(PathEntry basepath, File fileobj) throws FileNotFoundException {
		super(basepath);
		File[] f = fileobj.listFiles();
		if (f == null) {
			throw new FileNotFoundException("!! Folder not accessible");
		}
		file_iter = Arrays.asList(f).iterator();
	}

	public DirLister(PathEntry entry) throws FileNotFoundException {
		this(entry, new File(entry.getPath()));
	}

	protected void getNext() throws IOException {
		while (next_entry == null && file_iter.hasNext()) {
			long t2 = new Date().getTime();
			File f = file_iter.next();
			Assertion.assertAssertionError(f.getPath().startsWith(getBasePath().getPath()));
			Assertion.assertAssertionError(! f.getPath().equals(getBasePath().getPath() + "."));
			Assertion.assertAssertionError(! f.getPath().equals(getBasePath().getPath() + ".."));
			next_entry = new PathEntry(f);
			if (isCsumRequested() && next_entry.isFile()) {
				next_entry.setCsumAndClose(next_entry.getInputStream());
			}
			long t3 = new Date().getTime();
			if (t3-t2 > 1000) {
				writelog("getNext() too long: " + (t3-t2) + ", path=<" + next_entry.getPath() + ">");
			}
		}
	}

	private Set<String> pathnameUniquenessChecker = new HashSet<String> ();
	private void getNextWithIntegrityCheck() throws IOException {
		if (next_entry != null) { return; }
		getNext();
		if (next_entry != null) {
			Assertion.assertIOException(!pathnameUniquenessChecker.contains(next_entry.getPath()),
					"!! duplicate pathname: " + next_entry.getPath()
					);
			pathnameUniquenessChecker.add(next_entry.getPath());
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
		return next_entry != null;
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

		current_entry = next_entry;
		next_entry = null;
		return current_entry;
	}

	@Override
	public InputStream getInputStream(PathEntry entry) throws IOException {
		Assertion.assertAssertionError(entry.isFile());
		Assertion.assertAssertionError(entry.getPath().startsWith(getBasePath().getPath()));
		return entry.getInputStream();
	}

	@Override
	public InputStream getInputStream() throws IOException {
		Assertion.assertAssertionError(current_entry != null);
		return getInputStream(current_entry);
	}

	@Override
	public Iterator<PathEntry> iterator() {
		return this;
	}

	private static void writelog(final String message) {
		System.out.println(String.format("%s %s", new Date().toString(), message));
	}

}
