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

import com.github.n_i_e.dirtreedb.debug.Debug;

public class LazyProxyDirTreeDBAccessorThread extends StackingNonPreemptiveThread {

	public LazyProxyDirTreeDBAccessorThread(LazyProxyDirTreeDBProvider prov, RunnableWithLazyProxyDirTreeDBProvider target) {
		super(new Runnable() {

			@Override
			public void run() {
				try {
					Debug.writelog("--- Open DB (1/3) ---");
					target.setProv(prov);
					target.openingHook();
					Debug.writelog("--- Open DB (2/3) ---");
					prov.openDBIfNot();
					Debug.writelog("--- Open DB (3/3) ---");
					threadHook();
					target.run();
				} catch (InterruptedException e) {
					Debug.writelog("--- Interrupted ---");
					return;
				} catch (Throwable e) {
					Debug.writelog("Crawler Reached StandardCrawler bottom due to Exception: " + e.toString());
					prov.getMessageWriter().writeError("Exception", "This may be a fatal trouble. Exiting.\n" + e.toString());
					e.printStackTrace();
					System.exit(1);
				} finally {
					Debug.writelog("--- Close DB (1/3) ---");
					try {
						prov.closeDBIfPossible();
					} catch (Throwable e) {
						Debug.writelog("Failed closing DB file");
						prov.getMessageWriter().writeError("Error", "Failed closing DB file. This is may be a fatal trouble. Exiting.\n" + e.toString());
						e.printStackTrace();
						System.exit(1);
					}
					Debug.writelog("--- Close DB (2/3) ---");
					target.closingHook();
					Debug.writelog("--- Close DB (3/3) ---");
				}

			}

		}, false);
	}

}
