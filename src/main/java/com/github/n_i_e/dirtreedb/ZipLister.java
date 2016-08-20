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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

public class ZipLister extends AbstractArchiveLister {

	private ZipInputStream instream;
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

	public ZipLister (PathEntry basepath, InputStream inf, String charset) throws IOException {
		super(basepath);
		Assertion.assertNullPointerException(charset != null);
		Assertion.assertNullPointerException(inf != null);
		instream = new ZipInputStream(inf, Charset.forName(charset));
	}

	public ZipLister (PathEntry basepath, InputStream inf) throws IOException {
		this(basepath, inf, charset);
	}

	public static String getCharset() {
		return charset;
	}

	public static void setCharset(String charset) {
		ZipLister.charset = charset;
	}

	@Override
	public InputStream getInputStream() {
		return instream;
	}

	@Override
	protected PathEntry getNext() throws IOException {
		ZipEntry z;
		try {
			z = instream.getNextEntry();
		} catch (ZipException e) {
			return null;
		}
		if (z == null) {
			close();
			return null;
		}
		int newtype = z.isDirectory() ? PathEntry.COMPRESSEDFOLDER : PathEntry.COMPRESSEDFILE;
		String s = z.getName();
		s = s.replace("\\", "/");

		PathEntry next_entry = new PathEntry(getBasePath().getPath() + "/" + s, newtype);
		next_entry.setDateLastModified(z.getTime());
		next_entry.setStatus(PathEntry.DIRTY);
		next_entry.setSize(z.getSize());
		next_entry.setCompressedSize(z.getCompressedSize());
		if (isCsumRequested() && newtype == PathEntry.COMPRESSEDFILE) {
			try {
				next_entry.setCsum(instream);
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
		instream.close();
	}

}
