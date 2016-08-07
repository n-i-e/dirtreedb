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

import java.util.ArrayList;
import java.util.Iterator;

public class AsynchronousProducerConsumerIterator<T> implements Iterator<T>, Iterable<T> {

	private ArrayList<T> buffer = new ArrayList<T>();
	private boolean isOpen = true;

	public synchronized boolean hasNext() {
		return buffer.size() > 0;
	}

	public synchronized T next() {
		if (buffer.size() == 0) {
			return null;
		} else {
			T result = buffer.get(0);
			buffer.remove(0);
			return result;
		}
	}

	public synchronized Iterator<T> iterator() {
		return this;
	}

	public synchronized int size() {
		return buffer.size();
	}

	public synchronized void add(T op) {
		Assertion.assertNullPointerException(isOpen, "!! AsynchronousProducerConsumerIterator already closed");
		buffer.add(op);
	}

	public synchronized void close() {
		isOpen = false;
	}

}
