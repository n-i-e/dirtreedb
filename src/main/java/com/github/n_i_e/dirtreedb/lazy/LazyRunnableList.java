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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.github.n_i_e.dirtreedb.Assertion;

@Deprecated
public class LazyRunnableList extends LazyRunnable {

	protected List<LazyRunnable> list =
			Collections.synchronizedList(new ArrayList<LazyRunnable>());

	@Override
	public void run() throws SQLException {
		Assertion.assertAssertionError((LazyThread)Thread.currentThread() != null);
		try {
			LazyRunnable r = list.get(0);
			try {
				r.setProv(getProv());
				r.run();
			} catch (InterruptedException e) {}
			LazyRunnable r2 = list.remove(0);
			Assertion.assertAssertionError(r2 == r);
		} catch (IndexOutOfBoundsException e) {}
	}

	// add() is called from outside a StackingNonPreemptiveThread.
	public void add(LazyRunnable target) {
		list.add(target);
	}

	// size() is called from outside a StackingNonPreemptiveThread.
	public int size() {
		return list.size();
	}
}
