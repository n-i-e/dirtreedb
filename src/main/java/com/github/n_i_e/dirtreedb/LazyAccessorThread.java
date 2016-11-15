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

import java.lang.Thread.State;
import java.sql.SQLException;

public abstract class LazyAccessorThread {

	protected class RunnerThread extends Thread {

		RunnerThread() {
			super();
		}

		public void run() {
			try {
				Debug.writelog("--- Start Scenario (1/2) ---");
				startingHook1();
				getConf().regist();
				startingHook2();
				Debug.writelog("--- Start Scenario (2/2) ---");
				try {
					LazyAccessorThread.this.run();
				} catch (SQLException e) {
					Debug.writelog("STDS Reached thread bottom due to SQLException: " + e.toString());
					writeWarning("SQLException",
							"SQLException caught. This might be a program bug, but usually you can ignore.\n" + e.toString());
					e.printStackTrace();
					try {
						Thread.sleep(10*1000);
					} catch (InterruptedException e1) {}
				}
			} catch (InterruptedException e) {
				Debug.writelog("--- Interrupted ---");
			} catch (Exception e) {
				Debug.writelog("Reached DirTreeDbAccessorThread bottom due to Exception: " + e.toString());
				writeError("Exception", e.toString());
				e.printStackTrace();
			} finally {
				Debug.writelog("--- End Scenario (1/2) ---");
				endingHook1();
				try {
					getConf().unregist();
				} catch (SQLException e) {
					writeError("SQLException", "Failed closing DB file. This may be a fatal trouble. Exiting.\n" + e.toString());
					e.printStackTrace();
					System.exit(1);
				}
				endingHook2();
				Debug.writelog("--- End Scenario (2/2) ---");
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

	private RunnerThread thread = null;

	public void start(RunnerThread thread) {
		this.thread = thread;
		thread.start();
	}

	public State getState() {
		if (thread == null) {
			return State.NEW;
		}
		return thread.getState();
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

	protected LazyProxyDirTreeDb getDb() {
		Assertion.assertAssertionError((LazyAccessorThread.RunnerThread)Thread.currentThread() != null);
		return getConf().getLazyDirTreeDb();
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
