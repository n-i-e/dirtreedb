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

import java.util.ArrayList;

public class NavigatableArrayList<T> extends ArrayList<T> {
	int cursor = -1;

	@Override
	public boolean add(T element) {
		while (size() > cursor + 1) {
			remove(cursor+1);
		}
		super.add(element);
		cursor++;
		return true;
	}

	public void navigateNext() {
		if (size() > cursor+1) {
			cursor++;
		}
	}

	public void navigatePrevious() {
		if (cursor > 0) {
			cursor--;
		}
	}

	public T get() {
		return get(cursor);
	}
}
