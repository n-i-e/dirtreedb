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
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

public class PathEntryListerFactory {

	private static abstract class PathEntryListerReturner {
		public abstract PathEntryLister get(PathEntry entry, InputStream instream)  throws IOException;
		public PathEntryLister get(PathEntry entry, File contentfile)  throws IOException {
			return get(entry, new BufferedInputStream(new FileInputStream(contentfile), 1*1024*1024));
		}
		public PathEntryLister getForFile(PathEntry entry) throws IOException {
			return get(entry, entry.getInputStream());
		}
	}
	private static HashMap<String, PathEntryListerReturner> getExtensionBindList() {
		HashMap<String, PathEntryListerReturner> result = new HashMap<String, PathEntryListerReturner> ();

		final PathEntryListerReturner zipR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new ZipLister(base, inf); }
			@Override public PathEntryLister getForFile(PathEntry base) throws IOException { return new ZipListerForFile(base); }
		};

		final PathEntryListerReturner uzipR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new ZipLister(base, inf, "utf-8"); }
			@Override public PathEntryLister getForFile(PathEntry base) throws IOException { return new ZipListerForFile(base, "utf-8"); }
		};

		final PathEntryListerReturner lzhR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new LzhLister(base, inf); }
		};

		final PathEntryListerReturner svnzR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new TemporaryFileArchiveLister(base, inf); }
			@Override public PathEntryLister get(PathEntry base, File content) throws IOException { return new SevenZipListerForFile(base, content); }
			@Override public PathEntryLister getForFile(PathEntry base) throws IOException { return new SevenZipListerForFile(base); }
		};

		final PathEntryListerReturner gzR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new GzLister(base, inf); }
		};

		final PathEntryListerReturner apacheAR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new ApacheCompressArchiveLister(base, inf); }
		};

		final PathEntryListerReturner apacheCR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new ApacheCompressCompressorLister(base, inf); }
		};

		final PathEntryListerReturner apacheCAR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new ApacheCompressCompressingArchiveLister(base, inf); }
		};

		final PathEntryListerReturner emlR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new EmlLister(base, inf); }
		};

		final PathEntryListerReturner msgR = new PathEntryListerReturner () {
			@Override public PathEntryLister get(PathEntry base, InputStream inf) throws IOException { return new MsgLister(base, inf); }
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
		result.put("eml", emlR);
		result.put("wdseml", emlR);
		result.put("msg", msgR);

		result.put("jar", uzipR);
		result.put("kmz", uzipR);

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

	public static PathEntryLister getInstance(PathEntry entry, InputStream instream) throws IOException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertNullPointerException(instream != null);
		Assertion.assertAssertionError(entry.isFile() || entry.isCompressedFile());

		for (Entry<String, PathEntryListerReturner> kv: getExtensionBindList().entrySet()) {
			String ext = kv.getKey();
			if (fileExtensionMatches(entry.getPath(), ext)) {
				return kv.getValue().get(entry, instream);
			}
		}

		instream.close();
		return new NullArchiveLister();
	}

	public static PathEntryLister getInstance(PathEntry entry, File contentpath) throws IOException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertNullPointerException(contentpath != null);
		Assertion.assertAssertionError(entry.isFile() || entry.isCompressedFile());

		for (Entry<String, PathEntryListerReturner> kv: getExtensionBindList().entrySet()) {
			String ext = kv.getKey();
			if (fileExtensionMatches(entry.getPath(), ext)) {
				return kv.getValue().get(entry, contentpath);
			}
		}

		return new NullArchiveLister();
	}

	public static PathEntryLister getInstance(PathEntry entry) throws IOException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertAssertionError(entry.isFolder() || entry.isFile() || entry.isCompressedFile());

		if (entry.isFolder()) {
			return new DirLister(entry);
		} else if (entry.isFile()) {
			for (Entry<String, PathEntryListerReturner> kv: getExtensionBindList().entrySet()) {
				String ext = kv.getKey();
				if (fileExtensionMatches(entry.getPath(), ext)) {
					return kv.getValue().getForFile(entry);
				}
			}
			return new NullArchiveLister();
		} else if (entry.isCompressedFile()) {
			return getInstance(entry, entry.getInputStream());
		} else {
			return new NullArchiveLister();
		}
	}

	public static boolean isCsumRecommended(PathEntry entry) {
		if (entry.isFile()) {
			if (fileExtensionMatches(entry.getPath(), "zip")
					|| fileExtensionMatches(entry.getPath(), "7z")
					) {
				return false;
			}
		}
		return true;
	}
}
