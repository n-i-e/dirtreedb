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
import com.github.n_i_e.dirtreedb.windows.IsWin32Idle;

public class LazyMaintainerThread extends StackingNonPreemptiveThread {

	LazyMaintainerThread(LazyUpdaterProvider prov) {
		super(new Runnable() {

			@Override
			public void run() {
				LazyMaintainerRunnable target = new LazyMaintainerRunnable();
				while (true) {
					try {
						Debug.writelog("--- Maintainer Main Loop ---");
						if (!IsWin32Idle.isWin32Idle()) {
							Debug.writelog("--- Maintainer Start Sleep ---");
							System.gc();
							while (!IsWin32Idle.isWin32Idle()) {
								threadWait();
								Thread.sleep(1000);
							}
							Debug.writelog("--- Maintainer End Sleep ---");
						}
						Debug.writelog("--- Maintainer Open DB (1/3) ---");
						target.openingHook();
						Debug.writelog("--- Maintainer Open DB (2/3) ---");
						prov.openDBIfNot();
						Debug.writelog("--- Maintainer Open DB (3/3) ---");
						threadWait();
						target.setProv(prov);
						target.run();
					} catch (InterruptedException e) {
						Debug.writelog("--- Maintainer Interrupted ---");
					} catch (Throwable e) {
						Debug.writelog("Crawler Reached StandardCrawler bottom due to Exception: " + e.toString());
						prov.getMessageWriter().writeError("Exception", "This may be a fatal trouble. Exiting.\n" + e.toString());
						e.printStackTrace();
						System.exit(1);
					} finally {
						Debug.writelog("--- Maintainer Close DB (1/3) ---");
						try {
							prov.closeDBIfPossible();
						} catch (Throwable e) {
							Debug.writelog("Failed closing DB file");
							prov.getMessageWriter().writeError("Error", "Failed closing DB file. This is may be a fatal trouble. Exiting.\n" + e.toString());
							e.printStackTrace();
							System.exit(1);
						}
						Debug.writelog("--- Maintainer Close DB (2/3) ---");
						target.closingHook();
						Debug.writelog("--- Maintainer Close DB (3/3) ---");
					}
				}
			}

		}, true);
	}

}
