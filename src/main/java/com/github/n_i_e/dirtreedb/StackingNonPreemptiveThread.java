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

import com.github.n_i_e.dirtreedb.debug.Debug;

public class StackingNonPreemptiveThread extends ThreadWithInterruptHook {

	private Runnable target;
	private boolean lowPriority;
	private static StackingLock lock = new StackingLock();

	public StackingNonPreemptiveThread(Runnable target, boolean lowPriority) {
		super();
		Assertion.assertNullPointerException(target != null);
		this.target = target;
		this.lowPriority = lowPriority;
	}

	public StackingNonPreemptiveThread(Runnable target) {
		this(target, false);
	}

	public void setTopPriority() {
		Assertion.assertAssertionError(Thread.currentThread() == this);
		Debug.writelog("--- Set Top Priority (1/3) ---");
		lock.unregist();
		Debug.writelog("--- Set Top Priority (2/3) ---");
		lock.regist();
		Debug.writelog("--- Set Top Priority (3/3) ---");
	}

	@Override
	public final void run() {
		try {
			Debug.writelog("--- Start Thread (1/2) ---");
			if (lowPriority) {
				lock.registLowPriority();
			} else {
				lock.regist();
			}
			Debug.writelog("--- Start Thread (2/2) ---");
			target.run();
		} catch (Throwable e) {
			Debug.writelog("Reached Thread bottom due to Exception: " + e.toString());
			e.printStackTrace();
		} finally {
			Debug.writelog("--- End Thread (1/2) ---");
			lock.unregist();
			Debug.writelog("--- End Thread (2/2) ---");
		}

	}

	// interrupt() replacement; because "real" thread interrupt causes H2 Database unrecoverable SQLException
	private boolean isInterrupted = false;

	@Override
	public synchronized void interrupt() {
		isInterrupted = true;
	}

	@Override
	public void interruptHook() throws InterruptedException {
		synchronized(this) {
			if (isInterrupted) {
				isInterrupted = false;
				throw new InterruptedException();
			}
		}
		lock.keep();
	}

	protected static void threadHook() throws InterruptedException {
		try {
			((ThreadWithInterruptHook)Thread.currentThread()).interruptHook();
		} catch (ClassCastException e) {
			Thread.sleep(0);
		}
	}

}
