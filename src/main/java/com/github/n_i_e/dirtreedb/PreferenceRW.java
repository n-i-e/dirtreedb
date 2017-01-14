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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.prefs.Preferences;

public class PreferenceRW {

	private static Set<IPreferenceSyncUpdate> updaters = new CopyOnWriteArraySet<IPreferenceSyncUpdate>();
	public static void regist(IPreferenceSyncUpdate updater) {
		Assertion.assertNullPointerException(updater != null);
		updaters.add(updater);
		updater.setDBFilePath(getDBFilePath());
		updater.setExtensionAvailabilityMap(getExtensionAvailabilityMap());
		updater.setNumCrawlingThreads(getNumCrawlingThreads());
		updater.setWindowsIdleSeconds(getWindowsIdleSeconds());
		updater.setCharset(getZipListerCharset());
	}

	public static boolean unregist(IPreferenceSyncUpdate updater) {
		Assertion.assertNullPointerException(updater != null);
		return updaters.remove(updater);
	}

	private static Preferences prefs = Preferences.userNodeForPackage(PreferenceRW.class);

	// DBFilePath

	private final static String DBFilePath_KEY = "DbFilePath";

	public static String getDBFilePath() {
		String def = System.getProperty("user.home") + "\\.dirtreedb\\dirtreedb.sqlite";
		String result = prefs.get(DBFilePath_KEY, def);
		File p = new File(result).getParentFile();
		p.mkdirs();
		return result;
	}

	public static void setDBFilePath(String newvalue) {
		prefs.put(DBFilePath_KEY, newvalue);
		for (IPreferenceSyncUpdate p: updaters) {
			p.setDBFilePath(newvalue);
		}
	}

	// WindowsIdleSeconds

	private final static String WindowsIdleSeconds_KEY = "WindowsIdleSeconds";

	public static int getWindowsIdleSeconds() {
		return Integer.parseInt(prefs.get(WindowsIdleSeconds_KEY, "300"));
	}

	public static void setWindowsIdleSeconds(int windowsIdleSeconds) {
		prefs.put(WindowsIdleSeconds_KEY, String.valueOf(windowsIdleSeconds));
		for (IPreferenceSyncUpdate p: updaters) {
			p.setWindowsIdleSeconds(windowsIdleSeconds);
		}
	}

	// NumCrawlingThreads

	private final static String NumCrawlingThreads_KEY = "NumCrawlingThreads";

	public static int getNumCrawlingThreads() {
		return Integer.parseInt(prefs.get(NumCrawlingThreads_KEY, "4"));
	}

	public static void setNumCrawlingThreads(int numCrawlingThreads) {
		prefs.put(NumCrawlingThreads_KEY, String.valueOf(numCrawlingThreads));
		for (IPreferenceSyncUpdate p: updaters) {
			p.setNumCrawlingThreads(numCrawlingThreads);
		}
	}

	// ZipListerCharset

	private final static String ZipListerCharset_KEY = "ZipListerCharset";

	public static String getZipListerCharset() {
		return prefs.get(ZipListerCharset_KEY, "windows-31j");
	}

	public static void setZipListerCharset(String newvalue) {
		prefs.put(ZipListerCharset_KEY, newvalue);
		for (IPreferenceSyncUpdate p: updaters) {
			p.setCharset(newvalue);
		}
	}

	// SwtFileFolderMenuSortOrder

	private final static String SwtFileFolderMenuSortOrder_KEY = "SwtFileFolderMenuSortOrder";

	public static String getSwtFileFolderMenuSortOrder() {
		String[] candidates = {
				"path", "path DESC",
				"datelastmodified", "datelastmodified DESC",
				"size", "size DESC",
				"compressedsize", "compressedsize DESC",
				"duplicate", "duplicate DESC",
				"dedupablesize", "dedupablesize DESC"
		};

		String def = "compressedsize DESC";
		String result = prefs.get(SwtFileFolderMenuSortOrder_KEY, def);
		if (Arrays.asList(candidates).contains(result)) {
			return result;
		} else {
			return def;
		}
	}

	public static void setSwtFileFolderMenuSortOrder(String newvalue) {
		prefs.put(SwtFileFolderMenuSortOrder_KEY, newvalue);
	}

	// SwtDuplicateMenuSortOrderR

	private final static String SwtDuplicateMenuSortOrderR_KEY = "SwtDuplicateMenuSortOrderR";

	public static String getSwtDuplicateMenuSortOrderR() {
		String[] candidates = {
				"path", "path DESC",
				"datelastmodified", "datelastmodified DESC",
				"size", "size DESC",
				"compressedsize", "compressedsize DESC",
		};

		String def = "compressedsize DESC";
		String result = prefs.get(SwtDuplicateMenuSortOrderR_KEY, def);
		if (Arrays.asList(candidates).contains(result)) {
			return result;
		} else {
			return def;
		}
	}

	public static void setSwtDuplicateMenuSortOrderR(String newvalue) {
		prefs.put(SwtDuplicateMenuSortOrderR_KEY, newvalue);
	}

	// SwtRootMenuSortOrder

	private final static String SwtRootMenuSortOrder_KEY = "SwtRootMenuSortOrder";

	public static String getSwtRootMenuSortOrder() {
		String[] candidates = {
				"path", "path DESC",
				"datelastmodified", "datelastmodified DESC",
				"size", "size DESC",
				"compressedsize", "compressedsize DESC",
		};

		String def = "path";
		String result = prefs.get(SwtRootMenuSortOrder_KEY, def);
		if (Arrays.asList(candidates).contains(result)) {
			return result;
		} else {
			return def;
		}
	}

	public static void setSwtRootMenuSortOrder(String newvalue) {
		prefs.put(SwtRootMenuSortOrder_KEY, newvalue);
	}

	// ExtensionAvailabilityMap

	private static final String ExtensionAvailabilityMap_KEY = "ArchiveListerExtensionAvailabilityList";

	private static final String[] disabledByDefaultExtensionList = {"jar"};

	public static HashMap<String, Boolean> getExtensionAvailabilityMap() {
		Set<String> d1 = PathEntryListerFactory.getExtensionList();
		Assertion.assertNullPointerException(d1 != null);
		Assertion.assertNullPointerException(disabledByDefaultExtensionList != null);
		for (String d2: disabledByDefaultExtensionList) {
			d1.remove(d2);
		}
		String def = String.join(",", d1);

		String r1 = prefs.get(ExtensionAvailabilityMap_KEY, def);
		ArrayList<String> r2 = new ArrayList<String>();
		for (String k: r1.split(",")) {
			r2.add(k);
		}

		HashMap<String, Boolean> result = new HashMap<String, Boolean>();
		for (String k: PathEntryListerFactory.getExtensionList()) {
			if (r2.contains(k)) {
				result.put(k, true);
			} else {
				result.put(k, false);
			}
		}

		return result;
	}

	public static void setExtensionAvailabilityMap(Map<String, Boolean> newvalue) {
		ArrayList<String> r1 = new ArrayList<String>();
		for (Entry<String, Boolean> k: newvalue.entrySet()) {
			if (k.getValue()) {
				r1.add(k.getKey());
			}
		}
		prefs.put(ExtensionAvailabilityMap_KEY, String.join(",", r1));
		for (IPreferenceSyncUpdate p: updaters) {
			p.setExtensionAvailabilityMap(getExtensionAvailabilityMap());
		}

	}
}
