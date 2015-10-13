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

@Deprecated
public class SuspendResumeController {
	private boolean _isSuspended = false;

	public synchronized void suspend() {
		_isSuspended = true;
	}

	public synchronized void resume() {
		_isSuspended = false;
		notifyAll();
	}

	public synchronized void waitWhileSuspended() throws InterruptedException {
		while (_isSuspended) {
			wait();
		}
	}

	public synchronized boolean isSuspended() {
		return _isSuspended;
	}
}
