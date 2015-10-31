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

public class IsWin32Idle {

	private static int windowsIdleSeconds = 30;

	public static int getWindowsIdleSeconds() {
		return windowsIdleSeconds;
	}

	public static void setWindowsIdleSeconds(int windowsIdleSeconds) {
		IsWin32Idle.windowsIdleSeconds = windowsIdleSeconds;
	}

	public static boolean isWin32Idle() {
		if (getWindowsIdleSeconds() <= 0) {
			return true;
		} else {
			try {
				int t = getWin32IdleTime();
				if (t > getWindowsIdleSeconds()*1000) {
					return true;
				} else {
					return false;
				}
			} catch (GetLastInputInfoException e) {
				return false;
			}
		}
	}

	private static int getWin32IdleTime() throws GetLastInputInfoException {
		return win32_GetTickCount() - win32_GetLastInputInfo();
	}

	private static int win32_GetTickCount() {
		Kernel32 kernel32 = Kernel32.INSTANCE;
		return kernel32.GetTickCount();
	}

	private static int win32_GetLastInputInfo() throws GetLastInputInfoException {
		User32 user32 = User32.INSTANCE;
		LASTINPUTINFO lii = new LASTINPUTINFO();
		int err = user32.GetLastInputInfo(lii);
		if (err == 0) {
			throw new GetLastInputInfoException("!! GetLastInputInfo failure");
		}
		return lii.dwTime;
	}

	public static class GetLastInputInfoException extends Exception {
		GetLastInputInfoException(String s) {
			super(s);
		}
	}

}
