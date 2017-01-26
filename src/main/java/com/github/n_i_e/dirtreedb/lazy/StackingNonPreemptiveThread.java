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

import java.util.Stack;
import java.util.concurrent.locks.ReentrantLock;

import com.github.n_i_e.dirtreedb.Assertion;
import com.github.n_i_e.dirtreedb.debug.Debug;

public class StackingNonPreemptiveThread extends Thread {

	private Runnable target;
	private boolean lowPriority;
//	private static StackingLock lock = new StackingLock();
	private static ReentrantLock lock = new ReentrantLock();
	private static Stack<Thread> stack = new Stack<Thread>(); // ending of buffer has high priority

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
		Assertion.assertAssertionError(! lowPriority);
		synchronized (stack) {
			Debug.writelog("--- Set Top Priority (1/3) ---");
			int i = stack.indexOf(this);
			if (i<stack.size()-1) {
				stack.remove(i);
			}
			Debug.writelog("--- Set Top Priority (2/3) ---");
			stack.push(this);
			Debug.writelog("--- Set Top Priority (3/3) ---");
		}
	}

	@Override
	public final void run() {
		Assertion.assertAssertionError(this == Thread.currentThread());
		try {
			synchronized (stack) {
				Debug.writelog("--- Start Thread (1/3) ---");
				Assertion.assertAssertionError(stack.indexOf(this) == -1);
				if (lowPriority) {
					stack.insertElementAt(this, 0);
					Assertion.assertAssertionError(stack.indexOf(this) == 0);
				} else {
					stack.push(this);
					Assertion.assertAssertionError(stack.indexOf(this) == stack.size() - 1);
				}
				Debug.writelog("--- Start Thread (2/3) ---");
			}
			try {
				if (lock.isLocked()) {
					Debug.writelog("Thread is locked, waiting for Start Thread 3/3 ...");
				}
				lock.lock();
				Debug.writelog("--- Start Thread (3/3) ---");
				target.run();
			} finally {
				lock.unlock();
			}
		} catch (Throwable e) {
			Debug.writelog("Reached Thread bottom due to Exception: " + e.toString());
			e.printStackTrace();
		} finally {
			synchronized (stack) {
				Debug.writelog("--- End Thread (1/2) ---");
				int i = stack.indexOf(this);
				Assertion.assertAssertionError(i >= 0);
				stack.remove(i);
				Debug.writelog("--- End Thread (2/2) ---");
			}
		}

	}

	// interrupt() replacement; because "real" thread interrupt causes H2 Database unrecoverable SQLException
	private boolean isInterrupted = false;

	@Override
	public synchronized void interrupt() {
		isInterrupted = true;
	}

	public static void threadHook() throws InterruptedException {
		try {
			StackingNonPreemptiveThread thread = (StackingNonPreemptiveThread)Thread.currentThread();
			synchronized(thread) {
				if (thread.isInterrupted) {
					thread.isInterrupted = false;
					throw new InterruptedException();
				}
			}
			while (true) {
				while (! stack.peek().isAlive()) {
					stack.pop();
				}
				if (stack.peek() == thread) {
					break;
				} else { // not highest priority
					Assertion.assertAssertionError(lock.getHoldCount() == 1, "!! lock has " + lock.getHoldCount());
					lock.unlock();
					lock.lock();
				}
			}
		} catch (ClassCastException e) {
			Thread.sleep(0);
		}
	}

}
