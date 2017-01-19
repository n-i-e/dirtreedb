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

package com.github.n_i_e.dirtreedb.lazy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.github.n_i_e.dirtreedb.Assertion;
import com.github.n_i_e.dirtreedb.DirTreeDBFactory;
import com.github.n_i_e.dirtreedb.IDirTreeDB;
import com.github.n_i_e.dirtreedb.IPreferenceSyncUpdate;
import com.github.n_i_e.dirtreedb.MessageWriter;
import com.github.n_i_e.dirtreedb.PreferenceRW;
import com.github.n_i_e.dirtreedb.debug.Debug;

public class LazyProxyDirTreeDBProvider implements IPreferenceSyncUpdate {

	private LazyProxyDirTreeDB db = null;
	private MessageWriter messagewriter = new MessageWriter() {
		@Override public void writeWarning(String title, String message) {}
		@Override public void writeMessage(String title, String message) {}
		@Override public void writeInformation(String title, String message) {}
		@Override public void writeError(String title, String message) {}
	};
	private String dbFilePath = null;
	private Map<String, Boolean> extensionAvailabilityMap = null;

	public LazyProxyDirTreeDBProvider() {
		PreferenceRW.regist(this);
	}

	/*
	 * provides thread instances
	 */

	public LazyProxyDirTreeDBMaintainerThread getMaintainerThread() {
		return new LazyProxyDirTreeDBMaintainerThread(this);
	}

	public LazyProxyDirTreeDBAccessorThread getThread(RunnableWithLazyProxyDirTreeDBProvider target) {
		return new LazyProxyDirTreeDBAccessorThread(this, target);
	}

	/*
	 * IPreferenceSyncUpdate API
	 */

	@Override
	public synchronized void setDBFilePath(String dbFilePath) {
		Assertion.assertNullPointerException(dbFilePath != null);
		this.dbFilePath = dbFilePath;
	}

	@Override
	public synchronized void setExtensionAvailabilityMap(Map<String, Boolean> extensionAvailabilityMap) {
		Assertion.assertNullPointerException(extensionAvailabilityMap != null);
		this.extensionAvailabilityMap = extensionAvailabilityMap;
	}

	@Override public void setNumCrawlingThreads(int numCrawlingThreads) {}
	@Override public void setWindowsIdleSeconds(int windowsIdleSeconds) {}
	@Override public void setCharset(String newvalue) {}

	/*
	 * setters and getters
	 */

	public void setMessageWriter(MessageWriter messagewriter) {
		Assertion.assertNullPointerException(messagewriter != null);
		this.messagewriter = messagewriter;
	}

	public MessageWriter getMessageWriter() {
		return messagewriter;
	}

	public Map<String, Boolean> getExtensionAvailabilityMap() {
		return extensionAvailabilityMap;
	}

	/*
	 * LazyProxyDirTreeDB handling APIs
	 */

	Set<Thread> threads = new HashSet<Thread> ();

	public LazyProxyDirTreeDB getDB() {
		Assertion.assertAssertionError(db != null);
		if (! threads.contains(Thread.currentThread())) {
			threads.add(Thread.currentThread());
		}
		return db;
	}

	public synchronized LazyProxyDirTreeDB openDBIfNot() throws ClassNotFoundException, SQLException, IOException {
		if (! threads.contains(Thread.currentThread())) {
			threads.add(Thread.currentThread());
		}
		if (db != null) {
			return db;
		}

		Assertion.assertNullPointerException(dbFilePath != null);
		Debug.writelog("DB file is: " + dbFilePath);

		try {
			IDirTreeDB singlethreaddb = DirTreeDBFactory.getDirTreeDB(dbFilePath);
			if (singlethreaddb == null) {
				final String errmsg = String.format("Cannot determine DB type for this file name: %s", dbFilePath);
				Debug.writelog(errmsg);
				messagewriter.writeError("Fatal Error", errmsg);
				throw new IOException(errmsg);
			}
			db = new LazyProxyDirTreeDB(singlethreaddb);
			return db;
		} catch (ClassNotFoundException e) {
			final String errmsg = String.format("Possibly jdbc driver is not installed on this environment: %s\n%s", dbFilePath, e.toString());
			Debug.writelog(errmsg);
			messagewriter.writeError("ClassNotFoundException", errmsg);
			e.printStackTrace();
			db = null;
			throw e;
		} catch (SQLException e) {
			final String errmsg = String.format("Possibly DB file is broken: %s\n%s", dbFilePath, e.toString());
			Debug.writelog(errmsg);
			messagewriter.writeError("SQLException", errmsg);
			e.printStackTrace();
			db = null;
			throw e;
		} catch (IOException e) {
			final String errmsg = String.format("Possibly you cannot write file: %s\n%s", dbFilePath, e.toString());
			Debug.writelog(errmsg);
			messagewriter.writeError("IOException", errmsg);
			e.printStackTrace();
			db = null;
			throw e;
		}
	}

	public synchronized void closeDBIfPossible() throws SQLException {
		Set<Thread> r = new HashSet<Thread> ();
		for (Thread t: threads) {
			if (t.getState() == Thread.State.TERMINATED || t == Thread.currentThread()) {
				r.add(t);
			}
		}

		for (Thread t: r) {
			threads.remove(t);
		}

		if (threads.size() == 0) {
			LazyProxyDirTreeDB d = db;
			db = null;
			d.close();
		}
	}
}
