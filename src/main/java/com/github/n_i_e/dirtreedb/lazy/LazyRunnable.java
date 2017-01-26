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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import com.github.n_i_e.dirtreedb.Assertion;
import com.github.n_i_e.dirtreedb.MessageWriter;
import com.github.n_i_e.dirtreedb.RunnableWithException2;

public abstract class LazyRunnable implements RunnableWithException2<SQLException, InterruptedException> {

	public void openingHook() {}
	public void closingHook() {}

	private LazyUpdaterProvider prov = null;

	public LazyUpdaterProvider getProv() {
		return prov;
	}

	public void setProv(LazyUpdaterProvider prov) {
		this.prov = prov;
	}

	public boolean isProvReady() {
		return prov != null ? true : false;
	}

	public MessageWriter getMessageWriter() {
		Assertion.assertNullPointerException(prov != null);
		return prov.getMessageWriter();
	}

	public Map<String, Boolean> getExtensionAvailabilityMap() {
		Assertion.assertNullPointerException(prov != null);
		return prov.getExtensionAvailabilityMap();
	}

	public LazyUpdater getDB() {
		Assertion.assertNullPointerException(prov != null);
		return prov.getDB();
	}

	public LazyUpdater openDBIfNot() throws ClassNotFoundException, SQLException, IOException {
		return prov.openDBIfNot();
	}

	public void closeDBIfPossible() throws SQLException {
		prov.closeDBIfPossible();
	}

	public void threadHook() throws InterruptedException {
		StackingNonPreemptiveThread.threadHook();
	}
}
