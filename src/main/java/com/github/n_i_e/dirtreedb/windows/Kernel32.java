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

package com.github.n_i_e.dirtreedb.windows;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.WString;

public interface Kernel32 extends Library {
	Kernel32 INSTANCE = (Kernel32) Native.loadLibrary("kernel32", Kernel32.class);
	int GetCompressedFileSizeA(String lpFileName, int[] lpFileSizeHigh);
	int GetCompressedFileSizeW(WString lpFileName, int[] lpFileSizeHigh);
	int GetLastError();
	int GetTickCount();
}
