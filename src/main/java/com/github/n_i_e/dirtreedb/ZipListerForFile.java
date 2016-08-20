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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipListerForFile extends AbstractArchiveLister {

	private ZipFile zipfile;
	private Enumeration<? extends ZipEntry> zipentries;
	private static String charset = "windows-31j";

	static {
		PreferenceRW.regist(new IPreferenceSyncUpdate() {
			@Override public void setDbFilePath(String dbFilePath) {}
			@Override public void setExtensionAvailabilityMap(Map<String, Boolean> extensionAvailabilityMap) {}
			@Override public void setNumCrawlingThreads(int numCrawlingThreads) {}
			@Override public void setWindowsIdleSeconds(int windowsIdleSeconds) {}
			@Override public void setCharset(String newvalue) {
				ZipLister.setCharset(newvalue);
			}
		});
	}

	public ZipListerForFile (PathEntry basepath, File contentpath, String charset) throws IOException {
		super(basepath);
		Assertion.assertNullPointerException(charset != null);
		zipfile = new ZipFile(contentpath, Charset.forName(charset));
		zipentries = zipfile.entries();
	}

	public ZipListerForFile (PathEntry basepath, String charset) throws IOException {
		this(basepath, new File(basepath.getPath()), charset);
		Assertion.assertAssertionError(basepath.isFile());
	}

	public ZipListerForFile (PathEntry basepath, File contentpath) throws IOException {
		this(basepath, contentpath, charset);
		Assertion.assertAssertionError(basepath.isFile());
	}

	public ZipListerForFile (PathEntry basepath) throws IOException {
		this(basepath, charset);
	}

	public static String getCharset() {
		return charset;
	}

	public static void setCharset(String charset) {
		ZipListerForFile.charset = charset;
	}

	private class BufferedInputStreamWithCascadingClose extends BufferedInputStream {
		public BufferedInputStreamWithCascadingClose(InputStream in) {
			super(in);
		}

		public BufferedInputStreamWithCascadingClose(InputStream in, int size) {
			super(in, size);
		}
		public void close() throws IOException {
			super.close();
			ZipListerForFile.this.close();
		}
	}

	private ZipEntry next_zip_entry = null;
	@Override
	public InputStream getInputStream() throws IOException {
		Assertion.assertNullPointerException(next_zip_entry != null);
		return new BufferedInputStreamWithCascadingClose(zipfile.getInputStream(next_zip_entry), 1*1024*1024);
	}

	@Override
	protected PathEntry getNext() throws IOException {
		if (! zipentries.hasMoreElements()) {
			return null;
		}
		while (true) {
			try {
				next_zip_entry = zipentries.nextElement();
				break;
			} catch (IllegalArgumentException e) {
			}
		}
		int newtype = next_zip_entry.isDirectory() ? PathEntry.COMPRESSEDFOLDER : PathEntry.COMPRESSEDFILE;
		String s = next_zip_entry.getName();
		s = s.replace("\\", "/");
		PathEntry next_entry = new PathEntry(getBasePath().getPath() + "/" + s, newtype);
		next_entry.setDateLastModified(next_zip_entry.getTime());
		next_entry.setStatus(PathEntry.DIRTY);
		next_entry.setSize(next_zip_entry.getSize());
		next_entry.setCompressedSize(next_zip_entry.getCompressedSize());
		if (isCsumRequested() && newtype == PathEntry.COMPRESSEDFILE) {
			try {
				next_entry.setCsumAndClose(zipfile.getInputStream(next_zip_entry));
			} catch (IOException e) { // possibly encrypted zip
				next_entry.setStatus(PathEntry.NOACCESS);
			}
		}
		if (next_entry.getSize() < 0) {
			next_entry.setSize(0);
		}
		if (next_entry.getCompressedSize() < 0) {
			next_entry.setCompressedSize(next_entry.getSize());
		}
		return next_entry;
	}

	@Override
	public void close() throws IOException {
		super.close();
		zipfile.close();
	}

}
