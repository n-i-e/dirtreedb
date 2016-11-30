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

import java.util.Iterator;
import java.util.Stack;

public class StackingLock {

	BinaryStateReentrantLock lock = new BinaryStateReentrantLock(true); // fair
	Stack<Thread> stack = new Stack<Thread> ();

	private synchronized boolean isInStack() {
		Iterator<Thread> iter = stack.iterator();
		boolean result = false;
		while (iter.hasNext()) {
			Thread i = iter.next();
			if (Thread.currentThread() == i) {
				result = true;
			} else if (i.getState() == Thread.State.TERMINATED) {
				Assertion.assertAssertionError(!lock.isOwner(i));
				iter.remove();
			}
		}
		return result;
	}

	public synchronized int size() {
		Iterator<Thread> iter = stack.iterator();
		while (iter.hasNext()) {
			Thread i = iter.next();
			if (i.getState() == Thread.State.TERMINATED) {
				Assertion.assertAssertionError(!lock.isOwner(i));
				iter.remove();
			}
		}
		return stack.size();
	}

	public void regist() {
		lock.lock();
		if (!isInStack()) {
			stack.push(Thread.currentThread());
		}
		lock.unlock();
	}

	public void registLowPriority() {
		lock.lock();
		while (true) {
			if (isInStack()) {
				lock.unlock();
				lock.lock();
				if (Thread.currentThread() == stack.peek()) {
					return;
				}
			} else {
				stack.add(0, Thread.currentThread());
				return;
			}
		}
	}

	public synchronized void unregist() {
		Iterator<Thread> iter = stack.iterator();
		while (iter.hasNext()) {
			Thread i = iter.next();
			if (Thread.currentThread() == i) {
				iter.remove();
				unlock();
				return;
			}
		}
	}

	public void lock() {
		lock.lock();
		while (true) {
			Assertion.assertAssertionError(isInStack());
			lock.unlock();
			lock.lock();
			if (Thread.currentThread() == stack.peek()) {
				return;
			}
		}
	}

	public void unlock() {
		lock.unlock();
	}

	public void keep() {
		lock();
	}
}
