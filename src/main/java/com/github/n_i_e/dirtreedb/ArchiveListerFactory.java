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
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

public class ArchiveListerFactory {

	private interface ArchiveListerReturner {
		public IArchiveLister get(PathEntry entry, InputStream instream)  throws IOException;
	}
	private static HashMap<String, ArchiveListerReturner> getExtensionBindList() {
		HashMap<String, ArchiveListerReturner> result = new HashMap<String, ArchiveListerReturner> ();

		final ArchiveListerReturner zipR = new ArchiveListerReturner () {
			public IArchiveLister get(PathEntry base, InputStream inf) throws IOException { return new ZipLister(base, inf); }
		};

		final ArchiveListerReturner uzipR = new ArchiveListerReturner () {
			public IArchiveLister get(PathEntry base, InputStream inf) throws IOException { return new ZipLister(base, inf, "utf-8"); }
		};

		final ArchiveListerReturner lzhR = new ArchiveListerReturner () {
			public IArchiveLister get(PathEntry base, InputStream inf) throws IOException { return new LzhLister(base, inf); }
		};

		final ArchiveListerReturner svnzR = new ArchiveListerReturner () {
			public IArchiveLister get(PathEntry base, InputStream inf) throws IOException { return new SevenZipArchiveLister(base); }
		};

		final ArchiveListerReturner gzR = new ArchiveListerReturner () {
			public IArchiveLister get(PathEntry base, InputStream inf) throws IOException { return new GzLister(base, inf); }
		};

		final ArchiveListerReturner apacheAR = new ArchiveListerReturner () {
			public IArchiveLister get(PathEntry base, InputStream inf) throws IOException { return new ApacheCompressArchiveLister(base, inf); }
		};

		final ArchiveListerReturner apacheCR = new ArchiveListerReturner () {
			public IArchiveLister get(PathEntry base, InputStream inf) throws IOException { return new ApacheCompressCompressorLister(base, inf); }
		};

		final ArchiveListerReturner apacheCAR = new ArchiveListerReturner () {
			public IArchiveLister get(PathEntry base, InputStream inf) throws IOException { return new ApacheCompressCompressingArchiveLister(base, inf); }
		};

		result.put("zip", zipR);
		result.put("lzh", lzhR);
		result.put("gz", gzR);
		result.put("ar", apacheAR);
		result.put("cpio", apacheAR);
		result.put("dump", apacheAR);
		result.put("tar", apacheAR);
		result.put("tbz", apacheCAR);
		result.put("tgz", apacheCAR);
		result.put("arj", apacheAR);
		result.put("7z", svnzR);
		result.put("bz2", apacheCR);
		result.put("xz", apacheCR);
		result.put("z", apacheCR);
		result.put("lzma", apacheCR);
		result.put("sz", apacheCR);

		result.put("jar", uzipR);

		return result;
	}

	public static boolean fileExtensionMatches(String path, String ext) {
		Assertion.assertNullPointerException(path != null);
		Assertion.assertNullPointerException(ext != null);
		if (("." + ext).equalsIgnoreCase(path.substring(path.length()-ext.length()-1))) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isArchivable(PathEntry entry) {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertAssertionError(entry.isFile() || entry.isCompressedFile());

		for (String ext: getExtensionBindList().keySet()) {
			if (fileExtensionMatches(entry.getPath(), ext)) {
				return true;
			}
		}
		return false;
	}

	public static Set<String> getExtensionList() {
		return getExtensionBindList().keySet();
	}

	public static IArchiveLister getArchiveLister(PathEntry entry, InputStream instream) throws IOException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertNullPointerException(instream != null);

		for (Entry<String, ArchiveListerReturner> kv: getExtensionBindList().entrySet()) {
			String ext = kv.getKey();
			if (fileExtensionMatches(entry.getPath(), ext)) {
				return kv.getValue().get(entry, instream);
			}
		}

		instream.close();
		return new NullArchiveLister();
	}
}
