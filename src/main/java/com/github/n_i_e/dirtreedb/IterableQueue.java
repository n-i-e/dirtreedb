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

/**
 * IterableQueueはFIFOキューを実装します。
 *
 * addメソッドを使うと、エレメント１つがenqueueされます。
 * Iteratorインターフェースのnextメソッドを使うと、エレメント１つがdequeueされます。
 * また、Iterableインターフェースを利用して、for each構文でdequeueすることもできます。
 *
 * キューにエレメントが何もない状態でnextを実行すると、nullが返ります。
 * ただし、このときcloseは行われません。
 * そのため、この状態からaddすれば、またnextを呼ぶことができます。
 *
 * closeを実行すると、キューが「クローズ」され、キューに残ったエレメントは全て消去されます。
 * クローズされたキューに新たなエレメントをaddすることはできません（NullPointerExceptionが返ります）。
 *
 * IterableQueue implements a FIFO queue.
 *
 * With the add method, one element is enqueued.
 * With the next method of the Iterator interface, one element is dequeued.
 * You can also dequeue with for each syntax using the Iterable interface.
 *
 * Executing next with no elements in the queue will return null.
 * However, close is not performed at this time.
 * Therefore, if you add from this state, you can also call next.
 *
 * When you execute close, the queue is "closed", and all elements remaining in the queue are deleted.
 * It is not possible to add a new element to the closed queue (NullPointerException is returned).
 *
 * @param <T>
 */
public class IterableQueue<T> implements Iterator<T>, Iterable<T>, Closeable {

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
		Assertion.assertNullPointerException(isOpen, "!! IterableQueue already closed");
		buffer.add(op);
	}

	public synchronized void close() {
		isOpen = false;
		while (hasNext()) {
			next();
		}
	}

}
