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

import java.sql.SQLException;
import java.util.Date;

public abstract class LazyAccessorThread {

	public class RunnerThread extends Thread {

		RunnerThread() {
			super();
		}

		public void run() {
			try {
				writelog("--- Start Scenario (1/2) ---");
				startingHook1();
				getConf().regist();
				startingHook2();
				writelog("--- Start Scenario (2/2) ---");
				try {
					LazyAccessorThread.this.run();
				} catch (SQLException e) {
					writelog("STDS Reached thread bottom due to SQLException: " + e.toString());
					writeWarning("SQLException",
							"SQLException caught. This might be a program bug, but usually you can ignore.\n" + e.toString());
					e.printStackTrace();
					try {
						Thread.sleep(10*1000);
					} catch (InterruptedException e1) {}
				}
			} catch (InterruptedException e) {
				writelog("--- Interrupted ---");
			} catch (Exception e) {
				writelog("Reached DirTreeDbAccessorThread bottom due to Exception: " + e.toString());
				writeError("Exception", e.toString());
				e.printStackTrace();
			} finally {
				writelog("--- End Scenario (1/2) ---");
				endingHook1();
				try {
					getConf().unregist();
				} catch (SQLException e) {
					writeError("SQLException", "Failed closing DB file. This may be a fatal trouble. Exiting.\n" + e.toString());
					e.printStackTrace();
					System.exit(1);
				}
				endingHook2();
				writelog("--- End Scenario (2/2) ---");
			}
		}

		public void threadHook() throws InterruptedException {
			LazyAccessorThread.this.threadHook();
		}
	}

	private LazyAccessorThreadRunningConfig conf;

	protected LazyAccessorThreadRunningConfig getConf() {
		return conf;
	}

	public LazyAccessorThread(LazyAccessorThreadRunningConfig conf) {
		Assertion.assertNullPointerException(conf != null);
		this.conf = conf;
	}

	public void start(RunnerThread thread) {
		thread.start();
	}

	public void start() {
		new RunnerThread().start();
	}

	public abstract void run() throws Exception;
	public void startingHook1() {}
	public void startingHook2() {}
	public void endingHook1() {}
	public void endingHook2() {}

	// hand-made interrupt() replacement; because "real" thread interrupt causes H2 Database unrecoverable SQLException
	private boolean isInterrupted = false;

	public synchronized void interrupt() {
		isInterrupted = true;
	}

	protected synchronized boolean isInterrupted() {
		Assertion.assertAssertionError((LazyAccessorThread.RunnerThread)Thread.currentThread() != null);
		return isInterrupted;
	}

	protected LazyProxyDirTreeDb getDb() throws SQLException {
		Assertion.assertAssertionError((LazyAccessorThread.RunnerThread)Thread.currentThread() != null);
		return getConf().getLazyDirTreeDb();
	}

	protected AbstractDirTreeDb getSingleThreadDB() throws SQLException {
		Assertion.assertAssertionError((LazyAccessorThread.RunnerThread)Thread.currentThread() != null);
		return getConf().getSingleThreadDirTreeDb();
	}

	protected StackingLock getLock() {
		Assertion.assertAssertionError((LazyAccessorThread.RunnerThread)Thread.currentThread() != null);
		return getConf().getLock();
	}

	public void threadHook() throws InterruptedException {
		if (isInterrupted()) {
			throw new InterruptedException("!! DirTreeDBAccessibleThread interrupted");
		}
		getLock().lock();
	}

	protected static void writelog(final String message) {
		Date now = new Date();
		System.out.print(now);
		System.out.print(" ");
		System.out.println(message);
	}

	protected void writeMessage(String title, String message) {
		getConf().getMessageWriter().writeMessage(title, message);
	}

	protected void writeInformation(String title, String message) {
		getConf().getMessageWriter().writeInformation(title, message);
	}

	protected void writeWarning(String title, String message) {
		getConf().getMessageWriter().writeWarning(title, message);
	}

	protected void writeError(String title, String message) {
		getConf().getMessageWriter().writeError(title, message);
	}

}
