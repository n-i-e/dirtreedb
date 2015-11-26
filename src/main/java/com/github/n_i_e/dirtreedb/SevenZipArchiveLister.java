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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;

public class SevenZipArchiveLister extends AbstractArchiveLister {
	SevenZFile sevenzfile;

	public SevenZipArchiveLister(PathEntry basepath, InputStream inf) throws IOException {
		super(basepath);
		Assertion.assertIOException(basepath.isFile() || basepath.isCompressedFile());
		if (basepath.isFile()) {
			sevenzfile = new SevenZFile(new File(basepath.getPath()));
		} else {
			File toFile = File.createTempFile("DTDB", "temp.7z");
			Assertion.assertNullPointerException(toFile != null);
			Assertion.assertAssertionError(toFile.canWrite());
			toFile.deleteOnExit();
			Files.copy(inf, toFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			inf.close();
			sevenzfile = new SevenZFile(toFile);
		}
	}

	public InputStream getInputStream() {
		return new SevenZipInputStream(sevenzfile);
	}

	protected void getNext(boolean csum) throws IOException {
		if (next_entry != null) {
			return;
		}
		SevenZArchiveEntry z = sevenzfile.getNextEntry();
		if (z == null) {
			return;
		}
		int newtype = z.isDirectory() ? PathEntry.COMPRESSEDFOLDER : PathEntry.COMPRESSEDFILE;
		String s = z.getName();
		s = s.replace("\\", "/");
		if (z.isDirectory() && !s.endsWith("/")) {
			s = s + "/";
		}
		next_entry = new PathEntry(basepath.getPath() + "/" + s, newtype);
		next_entry.setDateLastModified(z.getLastModifiedDate().getTime());
		next_entry.setStatus(PathEntry.DIRTY);
		next_entry.setSize(z.getSize());
		next_entry.setCompressedSize(z.getSize());
		if (csum && newtype == PathEntry.COMPRESSEDFILE) {
			next_entry.setCsum(new SevenZipInputStream(sevenzfile));
		}
		if (next_entry.getSize() < 0) {
			next_entry.setSize(0);
		}
		if (next_entry.getCompressedSize() < 0) {
			next_entry.setCompressedSize(next_entry.getSize());
		}
	}
}
