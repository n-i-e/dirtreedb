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

import java.util.concurrent.locks.ReentrantLock;

public class BinaryStateReentrantLock extends ReentrantLock {

	BinaryStateReentrantLock() {
		super();
	}

	BinaryStateReentrantLock(boolean fair) {
		super(fair);
	}

	@Override
	public void lock() {
		if (! isHeldByCurrentThread()) {
			super.lock();
		}
	}

	@Override
	public void unlock() {
		while (isHeldByCurrentThread()) {
			super.unlock();
		}
	}

	public boolean isOwner(Thread thread) {
		return getOwner() == thread;
	}
}
