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
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

@Deprecated
public class LazyAccessorThreadRunningConfig implements IPreferenceSyncUpdate {

	private StackingLock lock = new StackingLock();
	private LazyProxyDirTreeDb lazydb = null;
	private MessageWriter messagewriter = null;
	private String dbFilePath = null;
	private Map<String, Boolean> extensionAvailabilityMap = null;

	public LazyAccessorThreadRunningConfig() {
		PreferenceRW.regist(this);
	}

	public Map<String, Boolean> getExtensionAvailabilityMap() {
		return extensionAvailabilityMap;
	}

	@Override public void setExtensionAvailabilityMap(Map<String, Boolean> extensionAvailabilityMap) {
		this.extensionAvailabilityMap = extensionAvailabilityMap;
	}

	public String getDbFilePath() {
		return dbFilePath;
	}

	@Override public void setDbFilePath(String dbFilePath) {
		Assertion.assertNullPointerException(dbFilePath != null);
		this.dbFilePath = dbFilePath;
	}

	public LazyProxyDirTreeDb getLazyDirTreeDb() {
		Assertion.assertNullPointerException(lazydb != null, "!! db is null");
		return lazydb;
	}

	public void setMessageWriter(MessageWriter messagewriter) {
		this.messagewriter = messagewriter;
	}

	public MessageWriter getMessageWriter() {
		return messagewriter;
	}

	public StackingLock getLock() {
		return lock;
	}

	private synchronized void openDbIfNot() throws ClassNotFoundException, SQLException, IOException {
		if (lazydb != null) {
			return;
		}
		Assertion.assertNullPointerException(lazydb == null);
		Assertion.assertNullPointerException(dbFilePath != null);
		writelog("DB file is: " + dbFilePath);

		try {
			IDirTreeDb singlethreaddb = DirTreeDbFactory.getDirTreeDb(dbFilePath);
			if (singlethreaddb == null) {
				final String errmsg = String.format("Cannot determine DB type for this file name: %s", dbFilePath);
				writelog(errmsg);
				messagewriter.writeError("Fatal Error", errmsg);
				return;
			}
			lazydb = new LazyProxyDirTreeDb(singlethreaddb);
		} catch (ClassNotFoundException e) {
			final String errmsg = String.format("Possibly jdbc driver is not installed on this environment: %s\n%s", dbFilePath, e.toString());
			writelog(errmsg);
			messagewriter.writeError("ClassNotFoundException", errmsg);
			e.printStackTrace();
			lazydb = null;
			throw e;
		} catch (SQLException e) {
			final String errmsg = String.format("Possibly DB file is broken: %s\n%s", dbFilePath, e.toString());
			writelog(errmsg);
			messagewriter.writeError("SQLException", errmsg);
			e.printStackTrace();
			lazydb = null;
			throw e;
		} catch (IOException e) {
			final String errmsg = String.format("Possibly you cannot write file: %s\n%s", dbFilePath, e.toString());
			writelog(errmsg);
			messagewriter.writeError("IOException", errmsg);
			e.printStackTrace();
			lazydb = null;
			throw e;
		}
	}

	public void closeDbIfPossible() throws SQLException {
		if (lock.size() == 0) {
			lock.regist();
			try {
				if (lazydb == null) {
					return;
				}
				if (lock.size() == 1) {
					writelog("Closing DB");
					LazyProxyDirTreeDb l = lazydb;
					lazydb = null;
					l.close();
				}
			} finally {
				lock.unregist();
			}
		}
	}

	public void regist() throws ClassNotFoundException, SQLException, IOException {
		lock.regist();
		openDbIfNot();
	}

	public void registLowPriority() throws ClassNotFoundException, SQLException, IOException {
		lock.registLowPriority();
		openDbIfNot();
	}

	public void unregist() throws SQLException {
		lock.unregist();
		closeDbIfPossible();
	}

	protected static void writelog(final String message) {
		Date now = new Date();
		System.out.print(now);
		System.out.print(" ");
		System.out.println(message);
	}

	@Override public void setNumCrawlingThreads(int numCrawlingThreads) {}
	@Override public void setWindowsIdleSeconds(int windowsIdleSeconds) {}
	@Override public void setCharset(String newvalue) {}
}
