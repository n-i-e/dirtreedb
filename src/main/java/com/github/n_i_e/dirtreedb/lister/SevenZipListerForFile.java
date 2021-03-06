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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;

import com.github.n_i_e.dirtreedb.Assertion;
import com.github.n_i_e.dirtreedb.PathEntry;

public class SevenZipListerForFile extends AbstractArchiveLister {
	private SevenZFile sevenzfile;

	public SevenZipListerForFile(PathEntry basepath, File contentpath) throws IOException {
		super(basepath);
		sevenzfile = new SevenZFile(contentpath);
	}

	public SevenZipListerForFile(PathEntry basepath) throws IOException {
		this(basepath, new File(basepath.getPath()));
		Assertion.assertAssertionError(basepath.isFile());
	}

	@Override
	public InputStream getInputStream() {
		return new SevenZipInputStream();
	}

	@Override
	protected PathEntry getNext() throws IOException {
		SevenZArchiveEntry z = sevenzfile.getNextEntry();
		if (z == null) {
			return null;
		}
		int newtype = z.isDirectory() ? PathEntry.COMPRESSEDFOLDER : PathEntry.COMPRESSEDFILE;
		String s = z.getName();
		if (s == null) {
			s = AbstractCompressorLister.getBasename(getBasePath());
		};
		s = s.replace("\\", "/");
		if (z.isDirectory() && !s.endsWith("/")) {
			s = s + "/";
		}
		PathEntry next_entry = new PathEntry(getBasePath().getPath() + "/" + s, newtype);
		next_entry.setDateLastModified(z.getLastModifiedDate().getTime());
		next_entry.setStatus(PathEntry.DIRTY);
		next_entry.setSize(z.getSize());
		next_entry.setCompressedSize(z.getSize());
		if (isCsumRequested() && newtype == PathEntry.COMPRESSEDFILE) {
			next_entry.setCsum(new SevenZipInputStream());
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
		sevenzfile.close();
	}

	public class SevenZipInputStream extends InputStream {

		@Override
		public int read() throws IOException {
			return sevenzfile.read();
		}

		@Override
		public int read(byte[] b) throws IOException {
			return sevenzfile.read(b);
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return sevenzfile.read(b, off, len);
		}

		@Override
		public void close() throws IOException {
			sevenzfile.close();
		}
	}
}
