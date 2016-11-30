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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;

public class AsynchronousProducerConsumerIteratorWithPriority<T> implements Iterator<T>, Iterable<T>, Closeable {

	private ArrayList<ArrayList<T>> buffer;
	private boolean isOpen = true;

	public boolean isOpen() {
		return isOpen;
	}

	public AsynchronousProducerConsumerIteratorWithPriority(int numPriorityLevels) {
		buffer = new ArrayList<ArrayList<T>>();
		for (int i=0; i<numPriorityLevels; i++) {
			buffer.add(new ArrayList<T>());
		}
	}

	public synchronized boolean hasNext() {
		for(ArrayList<T> element: buffer) {
			if (element.size() > 0) {
				return true;
			}
		}
		return false;
	}

	public synchronized T next() {
		for(ArrayList<T> element: buffer) {
			if (element.size() > 0) {
				T result = element.get(0);
				element.remove(0);
				return result;
			}
		}
		return null;
	}

	public synchronized Iterator<T> iterator() {
		return this;
	}

	public synchronized int size() {
		int result = 0;
		for(ArrayList<T> element: buffer) {
			result += element.size();
		}
		return result;
	}

	public synchronized int size(int priority) {
		return buffer.get(priority).size();
	}

	public synchronized void add(T op, int priority) {
		Assertion.assertAssertionError(priority < buffer.size());
		Assertion.assertNullPointerException(isOpen, "!! AsynchronousProducerConsumerIterator already closed");
		buffer.get(priority).add(op);
	}

	public void add(T op) {
		add(op, 0);
	}

	public synchronized void close() {
		isOpen = false;
		while (hasNext()) {
			next();
		}
	}

}
