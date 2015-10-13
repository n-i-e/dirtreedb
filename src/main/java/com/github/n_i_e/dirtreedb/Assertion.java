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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public class Assertion {

	public static void assertAssertionError(boolean condition, String message) {
		if (!condition) {
			throw new AssertionError(message);
		}
	}

	public static void assertAssertionError(boolean condition) {
		assertAssertionError(condition, "!! Assertion Failed");
	}

	public static void assertNullPointerException(boolean condition, String message) {
		if (!condition) {
			throw new NullPointerException(message);
		}
	}

	public static void assertNullPointerException(boolean condition) {
		assertNullPointerException(condition, "!! Assertion Failed");
	}

	public static void assertSQLException(boolean condition, String message) throws SQLException {
		if (!condition) {
			throw new SQLException(message);
		}
	}

	public static void assertSQLException(boolean condition) throws SQLException {
		assertSQLException(condition, "!! Assertion Failed");
	}

	public static void assertIOException(boolean condition, String message) throws IOException {
		if (!condition) {
			throw new IOException(message);
		}
	}

	public static void assertIOException(boolean condition) throws IOException {
		assertIOException(condition, "!! Assertion Failed");
	}

	public static void assertFileNotFoundException(boolean condition, String message) throws FileNotFoundException {
		if (!condition) {
			throw new FileNotFoundException(message);
		}
	}

	public static void assertFileNotFoundException(boolean condition) throws FileNotFoundException {
		assertFileNotFoundException(condition, "!! Assertion Failed");
	}

}
