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

@Deprecated
public class ProducerConsumerIterator<T> {
	private ArrayList<T> buffer;
	private boolean has_next;
	private int max_size;

	ProducerConsumerIterator(int maxSize) {
		buffer = new ArrayList<T>();
		has_next = true;
		max_size = maxSize;
	}

	ProducerConsumerIterator() {
		this(0);
	}

	public synchronized boolean hasNext() throws InterruptedException {
		while (buffer.size() == 0 && has_next == true) {
			wait();
		}
		boolean result;
		if (buffer.size() > 0) {
			result = true;
		} else {
			result = has_next;
		}
		notify();
		return result;
	}

	public synchronized T next() throws InterruptedException {
		while (buffer.size() == 0 && has_next == true) {
			wait();
		}
		T result;
		if (buffer.size() == 0) {
			result = null;
		} else {
			result = buffer.get(0);
			buffer.remove(0);
		}
		notify();
		return result;
	}

	public synchronized T previewNext() throws InterruptedException {
		T result;
		if (buffer.size() == 0) {
			result = null;
		} else {
			result = buffer.get(0);
		}
		notify();
		return result;
	}

	public synchronized int size() {
		return buffer.size();
	}

	public synchronized void add(T op) throws InterruptedException {
		if (has_next == false) {
			throw new NullPointerException("!! ProducerConsumerIterator already closed");
		}
		while (max_size > 0 && buffer.size() >= max_size) {
			wait();
		}
		buffer.add(op);
		notify();
	}

	public synchronized void close() {
		has_next = false;
		notify();
	}
}
