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

public class TemporaryFileArchiveLister extends AbstractArchiveLister {
	IArchiveLister baselister;

	public TemporaryFileArchiveLister(PathEntry basepath, InputStream inf) throws IOException {
		super(basepath);
		Assertion.assertNullPointerException(inf != null);
		if (basepath.isFile()) {
			inf.close();
			baselister = ArchiveListerFactory.getArchiveListerForFile(basepath);
		} else {
			File toFile = File.createTempFile("DTDB", getBasenameWithSuffix(basepath));
			Assertion.assertNullPointerException(toFile != null);
			Assertion.assertAssertionError(toFile.canWrite());
			toFile.deleteOnExit();
			Files.copy(inf, toFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			inf.close();
			baselister = ArchiveListerFactory.getArchiveListerForFile(new PathEntry(toFile));
		}
	}

	private static String getBasenameWithSuffix(PathEntry basepath) {
		int i1 = basepath.getPath().lastIndexOf("\\")+1;
		int i2 = basepath.getPath().lastIndexOf("/")+1;
		if (i2>i1) {
			i1 = i2;
		}
		return basepath.getPath().substring(i1);
	}

	public InputStream getInputStream() throws IOException {
		return baselister.getInputStream();
	}

	protected void getNext(boolean csum) throws IOException {
		if (next_entry != null) {
			return;
		}
		if (baselister.hasNext(csum)) {
			next_entry = baselister.next(csum);
		} else {
			next_entry = null;
		}
	}

	@Override
	public void close() throws IOException {
		baselister.close();
	}
}
