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

public class DBPathEntry extends PathEntry {
	private long pathid;
	private long parentid;
	private long rootid;

	public DBPathEntry() {
		super();
	}

	public DBPathEntry(String path, int type, long pathid, long parentid, long rootid) {
		super(path, type);
		this.pathid = pathid;
		this.parentid = parentid;
		this.rootid = rootid;
	}

	public DBPathEntry(DBPathEntry oldentry) {
		super(oldentry);
		this.pathid = oldentry.getPathId();
		this.parentid = oldentry.getParentId();
		this.rootid = oldentry.getRootId();
	}

	public long getPathId() {
		return pathid;
	}

	public void setPathId(long newid) {
		this.pathid = newid;
	}

	public long getParentId() {
		return parentid;
	}

	public void setParentId(long newid) {
		this.parentid = newid;
	}

	public long getRootId() {
		return rootid;
	}

	public void setRootId(long newid) {
		this.rootid = newid;
	}

	public boolean isRoot() {
		if (rootid == 0) {
			return true;
		} else {
			return false;
		}
	}
}
