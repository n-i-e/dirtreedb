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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.github.n_i_e.dirtreedb.StackingNonPreemptiveThread;

public class RunnableWithLazyProxyDirTreeDBProviderList extends RunnableWithLazyProxyDirTreeDBProvider {

	private List<RunnableWithLazyProxyDirTreeDBProvider> list =
			new CopyOnWriteArrayList<RunnableWithLazyProxyDirTreeDBProvider> ();

	private boolean isOpeningHookFinished = false;

	@Override
	public void openingHook() {
		if (list.size() > 0) {
			RunnableWithLazyProxyDirTreeDBProvider r = list.get(0);
			r.setProv(getProv());
			r.openingHook();
			isOpeningHookFinished = true;
		}
	}

	private RunnableWithLazyProxyDirTreeDBProvider nextRunnableForClosingHook = null;

	@Override
	public void run() throws SQLException {
		while (list.size() > 0) {
			RunnableWithLazyProxyDirTreeDBProvider r = list.get(0);
			try {
				((StackingNonPreemptiveThread)Thread.currentThread()).setTopPriority();
				closingHook();
				r.setProv(getProv());
				if (isOpeningHookFinished) {
					isOpeningHookFinished = false;
				} else {
					r.openingHook();
				}
				r.run();
				nextRunnableForClosingHook = r;
			} catch (InterruptedException e) {}
			list.remove(0);
		}
	}

	@Override
	public void closingHook() {
		if (nextRunnableForClosingHook != null) {
			nextRunnableForClosingHook.setProv(getProv());
			nextRunnableForClosingHook.closingHook();
			nextRunnableForClosingHook = null;
		}
	}

	public void add(RunnableWithLazyProxyDirTreeDBProvider target) {
		list.add(target);
	}

	public int size() {
		return list.size();
	}
}
