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

import java.util.HashSet;
import java.util.Set;

public class InterSetOperation {
	public static<T> Set<T> or(Set<T> arg1, Set<T> arg2) {
		Set<T> result = new HashSet<T>();
		for (T i: arg1) {
			result.add(i);
		}
		for (T i: arg2) {
			result.add(i);
		}
		return result;
	}

	public static<T> Set<T> and(Set<T> arg1, Set<T> arg2) {
		Set<T> result = new HashSet<T>();
		for (T i: arg1) {
			if (arg2.contains(i)) {
				result.add(i);
			}
		}
		return result;
	}

	public static<T> Set<T> minus(Set<T> arg1, Set<T> arg2) {
		Set<T> result = new HashSet<T>();
		for (T i: arg1) {
			if (! arg2.contains(i)) {
				result.add(i);
			}
		}
		return result;
	}

	public static<T> boolean include(Set<T> arg1, Set<T> arg2) {
		for (T i: arg2) {
			if (! arg1.contains(i)) {
				return false;
			}
		}
		return true;
	}
}
