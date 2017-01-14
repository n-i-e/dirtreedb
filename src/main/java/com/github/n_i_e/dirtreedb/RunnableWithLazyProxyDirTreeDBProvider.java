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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public abstract class RunnableWithLazyProxyDirTreeDBProvider implements RunnableWithException2<SQLException, InterruptedException> {

	public void openingHook() {}
	public void closingHook() {}

	private LazyProxyDirTreeDBProvider prov = null;

	public LazyProxyDirTreeDBProvider getProv() {
		return prov;
	}

	public void setProv(LazyProxyDirTreeDBProvider prov) {
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

	public LazyProxyDirTreeDB getDB() {
		Assertion.assertNullPointerException(prov != null);
		return prov.getDB();
	}

	public LazyProxyDirTreeDB openDBIfNot() throws ClassNotFoundException, SQLException, IOException {
		return prov.openDBIfNot();
	}

	public void closeDBIfPossible() throws SQLException {
		prov.closeDBIfPossible();
	}
}