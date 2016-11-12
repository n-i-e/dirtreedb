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

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractDirTreeDb {
	public abstract Statement createStatement() throws SQLException, InterruptedException;
	public abstract PreparedStatement prepareStatement(final String sql) throws SQLException, InterruptedException;
	public abstract void close() throws SQLException;
	public abstract DbPathEntry rsToPathEntry(ResultSet rs, String prefix) throws SQLException, InterruptedException;
	public abstract void insert(DbPathEntry basedir, PathEntry newentry) throws SQLException, InterruptedException;
	public abstract void update(DbPathEntry oldentry, PathEntry newentry) throws SQLException, InterruptedException;
	public abstract void updateStatus(DbPathEntry entry, int newstatus) throws SQLException, InterruptedException;
	public abstract void delete(DbPathEntry entry) throws SQLException, InterruptedException;
	public abstract void unsetClean(long pathid) throws SQLException, InterruptedException;
	public abstract void disable(DbPathEntry entry) throws SQLException, InterruptedException;
	public abstract void disable(DbPathEntry entry, PathEntry newentry) throws SQLException, InterruptedException;
	public abstract void updateParentId(DbPathEntry entry, long newparentid) throws SQLException, InterruptedException;
	public abstract void orphanize(DbPathEntry entry) throws SQLException, InterruptedException;
	public abstract void insertUpperLower(long upper, long lower, int distance) throws SQLException, InterruptedException;
	public abstract void deleteUpperLower(long upper, long lower) throws SQLException, InterruptedException;
	public abstract void insertEquality(long pathid1, long pathid2, long size, int csum) throws SQLException, InterruptedException;
	public abstract void deleteEquality(long pathid1, long pathid2) throws InterruptedException, SQLException;
	public abstract void updateEquality(long pathid1, long pathid2) throws InterruptedException, SQLException;
	public abstract void updateDuplicateFields(long pathid, long duplicate, long dedupablesize) throws InterruptedException, SQLException;

	public DbPathEntry rsToPathEntry(ResultSet rs) throws SQLException, InterruptedException {
		return rsToPathEntry(rs, "");
	}


	public abstract class Dispatcher {
		public static final int NONE = 0;

		public static final int LIST = 1;
		public static final int LIST_CSUM = 2;
		public static final int LIST_CSUM_FORCE = 3;

		protected int _list = NONE;
		public void setList(int listflag) { _list = listflag; }
		public boolean isList() { return _list == NONE ? false : true; }
		public boolean isListCsum() { return _list == LIST_CSUM || _list == LIST_CSUM_FORCE ? true : false; }
		public boolean isListCsumForce() { return _list == LIST_CSUM_FORCE ? true : false; }

		public static final int CSUM = 1;
		public static final int CSUM_FORCE = 2;

		protected int _csum = NONE;

		public void setCsum(int csumflag) { _csum = csumflag; }
		public boolean isCsum() { return _csum == NONE ? false : true; }
		public boolean isCsumForce() { return _csum == CSUM_FORCE ? true : false; }

		protected boolean _noChildInDb = false;
		public void setNoChildInDb(boolean noChildInDb) { _noChildInDb = noChildInDb; }
		public boolean isNoChildInDb() { return _noChildInDb; }

		protected Map<Long, String> reachableRoots = null;
		public Map<Long, String> getReachableRoots() { return reachableRoots; }
		public void setReachableRoots(Set<DbPathEntry> roots) {
			reachableRoots = new ConcurrentHashMap<Long, String>();
			if (roots != null) {
				for (DbPathEntry e: roots) {
					reachableRoots.put(e.getPathId(), e.getPath());
				}
			}
		}
		public void deleteReachableRoot(long rootid) {
			reachableRoots.remove(rootid);
		}
		public boolean isReachableRoot(long rootid) {
			if (reachableRoots == null) {
				return true; // always true when reachableRoots is undefined
			}
			return reachableRoots.containsKey(rootid);
		}
		public String getReachableRootPath(long rootid) {
			if (reachableRoots == null) { return null; }
			return reachableRoots.get(rootid);
		}
		public void checkRootAndDisable(final DbPathEntry entry) throws SQLException, InterruptedException {
			Assertion.assertNullPointerException(entry != null);
			if (isReachableRoot(entry.getRootId())) {
				String s = getReachableRootPath(entry.getRootId());
				if (s == null) {
					disable(entry);
				} else {
					if ((new File(s)).exists()) {
						disable(entry);
					} else {
						deleteReachableRoot(entry.getRootId());
					}
				}
			}
		}

		public abstract PathEntry dispatch(final DbPathEntry entry) throws IOException, InterruptedException, SQLException;
	}

}
