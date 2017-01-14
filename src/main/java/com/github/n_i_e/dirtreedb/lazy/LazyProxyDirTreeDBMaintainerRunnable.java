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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.github.n_i_e.dirtreedb.Assertion;
import com.github.n_i_e.dirtreedb.DBPathEntry;
import com.github.n_i_e.dirtreedb.InterSetOperation;
import com.github.n_i_e.dirtreedb.IsEol;
import com.github.n_i_e.dirtreedb.PathEntry;
import com.github.n_i_e.dirtreedb.lazy.LazyProxyDirTreeDB.Dispatcher;
import com.github.n_i_e.dirtreedb.lister.PathEntryListerFactory;
import com.github.n_i_e.dirtreedb.windows.IsWin32Idle;

class LazyProxyDirTreeDBMaintainerRunnable extends RunnableWithLazyProxyDirTreeDBProvider {

	private static final int UPDATE_QUEUE_SIZD_LOW_THRESHOLD = 9000;
	private static final int UPDATE_QUEUE_SIZD_HIGH_THRESHOLD = 10000;

	private static final int INSERTABLE_QUEUE_LOW_THRESHOLD = 50;
	private static final int INSERTABLE_QUEUE_HIGH_THRESHOLD = 100;
	private static final int RELAXED_INSERTABLE_QUEUE_SIZE_LOW_THRESHOLD = 950;
	private static final int RELAXED_INSERTABLE_QUEUE_SIZE_HIGH_THRESHOLD = 1000;

	private static final int DONT_INSERT_QUEUE_SIZE_LOW_THRESHOLD = 9000;
	private static final int DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD = 10000;

	private void writelog2(String message) {
		getDB().writelog2(message);
	}

	public void run() throws SQLException, InterruptedException {
		int cI=0, cD=0, cU=0;

		while (true) {
			getDB().consumeSomeUpdateQueue();
			beacon();
			while (getDB().getUpdateQueueSize(0) > UPDATE_QUEUE_SIZD_LOW_THRESHOLD) {
				getDB().consumeOneUpdateQueue();
				beacon();
			}

			if (scheduleInsertable[cI].isStartable()) {
				writelog2("--- scuedule layer 1 ---");
				if (scheduleInsertable[cI].isEol()) {
					cI++;
					if (cI >= scheduleInsertable.length) {
						cI = 0;
					}
				}
			} else {
				writelog2("--- SKIP scuedule layer 1 ---");
			}

			getDB().consumeSomeUpdateQueue();
			beacon();
			while (getDB().getUpdateQueueSize(0) > UPDATE_QUEUE_SIZD_LOW_THRESHOLD) {
				getDB().consumeOneUpdateQueue();
				beacon();
			}

			if (scheduleDontInsert[cD].isStartable()) {
				writelog2("--- scuedule layer 2 ---");
				if (scheduleDontInsert[cD].isEol()) {
					cD++;
					if (cD >= scheduleDontInsert.length) {
						cD = 0;
					}
				}
			} else {
				writelog2("--- SKIP scuedule layer 2 ---");
			}

			getDB().consumeSomeUpdateQueue();
			beacon();
			while (getDB().getUpdateQueueSize(0) > UPDATE_QUEUE_SIZD_LOW_THRESHOLD) {
				getDB().consumeOneUpdateQueue();
				beacon();
			}

			if (scheduleUpdate[cU].isStartable()) {
				writelog2("--- scuedule layer 3 ---");
				if (scheduleUpdate[cU].isEol()) {
					cU++;
					if (cU >= scheduleUpdate.length) {
						cU = 0;
					}
				}
			}
		}
	}

	long debugBeaconMessageTimer = 0L;
	private void beacon() throws InterruptedException {
		if (!IsWin32Idle.isWin32Idle()) {
			throw new InterruptedException();
		}
		long t = new Date().getTime();
		if (t - debugBeaconMessageTimer >= 10*60*1000) {
			debugBeaconMessageTimer = t;
			writelog2("beacon.");
		}
	}

	private abstract class Schedule implements IsEol {
		public abstract boolean isStartable() throws SQLException, InterruptedException;
		protected abstract long getQueueSizeLowThreshold();
		@Override public abstract boolean isEol() throws SQLException, InterruptedException;

		public abstract IsEol getQueueLimit();

		private long lastPathId = -1;

		public void setLastPathIdAvailable(boolean useLastPathId) {
			if (useLastPathId) {
				if (lastPathId == -2) {
					lastPathId = -1;
				}
			} else {
				lastPathId = -2;
			}
		}

		public boolean isLastPathIdAvailable() {
			if (lastPathId == -2) {
				return false;
			} else {
				return true;
			}
		}

		public void setLastPathId(long lastPathId) {
			this.lastPathId = lastPathId;
		}
		public long getLastPathId() {
			return lastPathId;
		}

		public void resetLastPathId() {
			if (isLastPathIdAvailable()) {
				lastPathId = -1;
			}
		}

	}

	private abstract class ScheduleInsertable extends Schedule {
		@Override public boolean isStartable() throws SQLException, InterruptedException {
			if (getDB().getUpdateQueueSize(0) > 0) {
				return false;
			}

			if (getDB().getInsertableQueueSize() >= getQueueSizeLowThreshold()) {
				return false;
			}

			Set<DBPathEntry> allRoots = getAllRoots();
			Set<Long> allRootIds = getIdsFromEntries(allRoots);
			Set<Long> dontAccessRootIds = getDB().getInsertableRootIdSet();

			if (!InterSetOperation.include(dontAccessRootIds, allRootIds)) {
				return true;
			} else {
				return false;
			}
		}

		protected long getQueueSizeLowThreshold() { return INSERTABLE_QUEUE_LOW_THRESHOLD; }

		protected long getQueueSizeHighThreshold() { return INSERTABLE_QUEUE_HIGH_THRESHOLD; }

		private final IsEol queueLimit = new IsEol() {
			@Override
			public boolean isEol() throws SQLException, InterruptedException {
				if (getDB().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZD_HIGH_THRESHOLD
						|| getDB().getUpdateQueueSize(1) >= UPDATE_QUEUE_SIZD_HIGH_THRESHOLD
						|| getDB().getInsertableQueueSize() >= getQueueSizeHighThreshold()) {
					return true;
				} else {
					return false;
				}
			}
		};
		@Override public IsEol getQueueLimit() { return queueLimit; }

		public int list(Set<Long> dontListRootIds, Set<DBPathEntry> rootmap, String typeStatusSubSQL, boolean hasNoChild)
				throws SQLException, InterruptedException {
			Dispatcher disp = getDB().getDispatcher();
			disp.setList(Dispatcher.LIST);
			disp.setCsum(Dispatcher.NONE);
			disp.setNoReturn(true);
			disp.setNoChildInDB(hasNoChild);
			disp.setReachableRoots(rootmap);

			String sql = "SELECT * FROM directory AS d1 WHERE "
					+ typeStatusSubSQL
					+ getSubSQLFromIds(dontListRootIds)
					+ " AND " + (hasNoChild ? "NOT " : "") + "EXISTS (SELECT * FROM directory WHERE parentid=d1.pathid)"
					+ " AND (d1.parentid=0 OR EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid ))"
					+ (isLastPathIdAvailable() ? " AND d1.pathid>? ORDER BY d1.pathid" : "")
					;
			//writelog2(sql);
			PreparedStatement ps = getDB().prepareStatement(sql);
			//writelog2(String.valueOf(lastPathId));
			if (isLastPathIdAvailable()) {
				ps.setLong(1, getLastPathId());
			}
			ResultSet rs = ps.executeQuery();
			writelog2("+++ list query finished +++");
			int count = 0;
			try {
				while (rs.next()) {
					DBPathEntry f = getDB().rsToPathEntry(rs);
					Assertion.assertAssertionError(f.isFolder()
							|| ((f.isFile() || f.isCompressedFile()) && PathEntryListerFactory.isArchivable(f)),
							"!! CANNOT LIST THIS ENTRY: " + f.getType() + " at " + f.getPath());
					try {
						disp.dispatch(f);
					} catch (IOException e) {}
					count++;
					if (isLastPathIdAvailable()) {
						setLastPathId(f.getPathId());
					}
					if (getQueueLimit().isEol()) {
						return count;
					}
				}
			} finally {
				rs.close();
				ps.close();
			}
			return count;
		}
	}

	private abstract class ScheduleDontInsert extends Schedule {
		@Override public boolean isStartable() {
			if (getDB().getDontInsertQueueSize() < getQueueSizeLowThreshold()) {
				return true;
			} else {
				return false;
			}
		}

		protected long getQueueSizeLowThreshold() { return DONT_INSERT_QUEUE_SIZE_LOW_THRESHOLD; }

		private final IsEol queueLimit = new IsEol() {
			@Override
			public boolean isEol() throws SQLException, InterruptedException {
				if (getDB().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZD_HIGH_THRESHOLD
						|| getDB().getUpdateQueueSize(1) >= UPDATE_QUEUE_SIZD_HIGH_THRESHOLD
						|| getDB().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD) {
					return true;
				} else {
					return false;
				}
			}
		};
		@Override public IsEol getQueueLimit() { return queueLimit; }

		public int csum(PreparedStatement ps, Set<DBPathEntry> reachableRoots)
				throws SQLException, InterruptedException {
			ResultSet rs = ps.executeQuery();
			writelog2("--- csum query finished ---");
			int count = 0;
			try {
				Dispatcher disp = getDB().getDispatcher();
				disp.setList(Dispatcher.NONE);
				disp.setCsum(Dispatcher.CSUM_FORCE);
				disp.setNoReturn(true);
				disp.setReachableRoots(reachableRoots);
				while (rs.next()) {
					DBPathEntry f = getDB().rsToPathEntry(rs);
					assert(f.isFile() || f.isCompressedFile());
					try {
						disp.dispatch(f);
					} catch (IOException e) {}
					count++;
					if (isLastPathIdAvailable()) {
						setLastPathId(f.getPathId());
					}
					if (queueLimit.isEol()) {
						break;
					}
				}
			} finally {
				rs.close();
				ps.close();
			}
			return count;
		}

		public int touch(PreparedStatement ps, Set<DBPathEntry> reachableRoots)
				throws SQLException, InterruptedException {
			ResultSet rs = ps.executeQuery();
			writelog2("--- touch query finished ---");
			int count = 0;
			try {
				Dispatcher disp = getDB().getDispatcher();
				disp.setList(Dispatcher.NONE);
				disp.setCsum(Dispatcher.NONE);
				disp.setNoReturn(true);
				disp.setReachableRoots(reachableRoots);
				while (rs.next()) {
					DBPathEntry f = getDB().rsToPathEntry(rs);
					assert(f.isFolder() || f.isFile());
					try {
						disp.dispatch(f);
					} catch (IOException e) {}
					count++;
					if (isLastPathIdAvailable()) {
						setLastPathId(f.getPathId());
					}
					if (queueLimit.isEol()) {
						break;
					}
				}
			} finally {
				rs.close();
				ps.close();
			}
			return count;
		}

		public int crawlEqualityUpdate(Set<DBPathEntry> rootmap)
				throws SQLException, InterruptedException {
			writelog2("--- equality ---");
			Dispatcher disp = getDB().getDispatcher();
			disp.setNoReturn(true);
			disp.setReachableRoots(rootmap);
			Statement stmt = getDB().createStatement();
			String sql = "SELECT * FROM equality ORDER BY datelasttested";
			ResultSet rs = stmt.executeQuery(sql);
			writelog2("--- equality query finished ---");
			int count = 0;
			try {
				while (rs.next()) {
					DBPathEntry p1 = getDB().getDBPathEntryByPathId(rs.getLong("pathid1"));
					DBPathEntry p2 = getDB().getDBPathEntryByPathId(rs.getLong("pathid2"));
					disp.checkEquality(p1, p2, disp.CHECKEQUALITY_UPDATE);
					count++;
					if (queueLimit.isEol()) {
						break;
					}
				}
			} finally {
				rs.close();
			}
			writelog2("--- equality finished count=" + count + " ---");
			return count;
		}

	}

	private abstract class ScheduleUpdate extends Schedule {

		@Override public boolean isStartable() throws SQLException, InterruptedException {
			if (getDB().getUpdateQueueSize() > 0) {
				return false;
			} else {
				return true;
			}
		}

		@Override protected long getQueueSizeLowThreshold() { return 0; }

		private final IsEol queueLimit = new IsEol() {
			@Override
			public boolean isEol() throws SQLException, InterruptedException {
				if (getDB().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZD_HIGH_THRESHOLD
						|| getDB().getUpdateQueueSize(1) >= UPDATE_QUEUE_SIZD_HIGH_THRESHOLD) {
					return true;
				} else {
					return false;
				}
			}
		};
		@Override public IsEol getQueueLimit() { return queueLimit; }

		public int unlistDisabledExtensions()
				throws SQLException, InterruptedException {
			ArrayList<String> ext = new ArrayList<String>();
			Map<String, Boolean> eal = getExtensionAvailabilityMap();
			for (Entry<String, Boolean> kv: eal.entrySet()) {
				if (! kv.getValue()) {
					ext.add("path LIKE '%." + kv.getKey() + "'");
				}
			}
			String dontArchiveExtSubSQL;
			if (ext.size() == 0) {
				writelog2("*** SKIP unlist disabled extensions (nothing to unlist) ***");
				return 0;
			} else {
				dontArchiveExtSubSQL = "AND (" + String.join(" OR ", ext) + ")";
				String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR type=3) " + dontArchiveExtSubSQL
						+ " AND EXISTS (SELECT * FROM directory AS d2 WHERE d2.parentid=d1.pathid)"
						+ " AND EXISTS (SELECT * FROM directory AS d3 WHERE d1.parentid=d3.pathid)";
				Statement stmt = getDB().createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				try {
					int count = 0;
					while (rs.next()) {
						DBPathEntry f = getDB().rsToPathEntry(rs);
						Assertion.assertAssertionError(f.isFile() || f.isCompressedFile());
						getDB().updateStatus(f, PathEntry.DIRTY);
						getDB().orphanizeChildren(f);
						count++;
						if (getQueueLimit().isEol()) {
							break;
						}
					}
					return count;
				} finally {
					rs.close();
					stmt.close();
				}
			}
		}

		public int orphanizeOrphansChildren() throws SQLException, InterruptedException {
			String sql = "SELECT * FROM directory AS d1 WHERE EXISTS "
					+ "(SELECT * FROM directory AS d2 WHERE d1.parentid=d2.pathid AND d2.parentid<>0 "
					+ "AND NOT EXISTS (SELECT * FROM directory AS d3 WHERE d2.parentid=d3.pathid))";
			PreparedStatement ps = getDB().prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			try {
				int count = 0;
				while (rs.next()) {
					DBPathEntry entry = getDB().rsToPathEntry(rs);
					getDB().orphanize(entry);
					count ++;
					if (getQueueLimit().isEol()) {
						break;
					}
				}
				return count;
			} finally {
				rs.close();
				ps.close();
			}
		}

	}

	private Set<Long> listFoldersWithChildrenFinished = null;

	Schedule[] scheduleInsertable = {
			new ScheduleInsertable() {
				private int repeatCounter=0;
				@Override public boolean isEol() throws SQLException, InterruptedException {
					writelog2("+++ list folders with children +++");
					setLastPathIdAvailable(false);
					Set<DBPathEntry> allRoots = getAllRoots();
					Set<Long> dontAccessRootIds = getDB().getInsertableRootIdSet();

					int count = list(dontAccessRootIds,  minus(allRoots, dontAccessRootIds), "type=0 AND status=1", false);
					writelog2("+++ list folders with children finished count=" + count + " +++");

					if (count>0 && repeatCounter < 3) {
						listFoldersWithChildrenFinished = null;
						repeatCounter++;
						return false;
					} else {
						if (count==0) {
							listFoldersWithChildrenFinished = dontAccessRootIds;
						} else {
							listFoldersWithChildrenFinished = null;
						}
						repeatCounter = 0;
						return true;
					}
				}
			},
			new ScheduleInsertable() {
				@Override protected long getQueueSizeLowThreshold() { return RELAXED_INSERTABLE_QUEUE_SIZE_LOW_THRESHOLD; }
				@Override protected long getQueueSizeHighThreshold() { return RELAXED_INSERTABLE_QUEUE_SIZE_HIGH_THRESHOLD; }
				private int repeatCounter=0;
				@Override public boolean isEol() throws SQLException, InterruptedException {
					writelog2("+++ list folders without children +++");
					setLastPathIdAvailable(false);
					Set<DBPathEntry> allRoots = getAllRoots();
					Set<Long> allRootIds = getIdsFromEntries(allRoots);
					Set<Long> dontAccessRootIds = getDB().getInsertableRootIdSet();

					int count = list(dontAccessRootIds,  minus(allRoots, dontAccessRootIds), "type=0 AND status=1", true);
					writelog2("+++ list folders without children finished count=" + count + " +++");

					if (count==0) {
						if (listFoldersWithChildrenFinished != null) {
							Set<Long> dontAccessRootIds2 =
									InterSetOperation.or(listFoldersWithChildrenFinished, dontAccessRootIds);
							if (! InterSetOperation.include(dontAccessRootIds2, allRootIds)) {
								setAllCleanFoldersDirty(dontAccessRootIds2);
							}
							listFoldersWithChildrenFinished = null;
						}
						return true;
					} else if (repeatCounter < 3) {
						return false;
					} else {
						repeatCounter = 0;
						return true;
					}
				}
			},
			new ScheduleInsertable() {
				@Override public boolean isEol() throws SQLException, InterruptedException {
					writelog2("+++ list files with children +++");
					setLastPathIdAvailable(false);
					Set<DBPathEntry> allRoots = getAllRoots();
					Set<Long> dontAccessRootIds = getDB().getInsertableRootIdSet();
					int count = list(dontAccessRootIds, minus(allRoots, dontAccessRootIds),
							"((type=1 OR type=3) AND (" + getArchiveExtSubSQL() + ")) AND status=1", false);
					writelog2("+++ list files with children finished count=" + count + " +++");
					return true;
				}
			},
			new ScheduleInsertable() {
				@Override public boolean isEol() throws SQLException, InterruptedException {
					writelog2("+++ list files without children +++");
					setLastPathIdAvailable(false);
					Set<DBPathEntry> allRoots = getAllRoots();
					Set<Long> dontAccessRootIds = getDB().getInsertableRootIdSet();
					int count = list(dontAccessRootIds, minus(allRoots, dontAccessRootIds),
							"((type=1 OR type=3) AND (" + getArchiveExtSubSQL() + ")) AND status=1", true);
					writelog2("+++ list files without children finished count=" + count + " +++");
					return true;
				}
			},
			new ScheduleInsertable() {
				@Override public boolean isEol() throws SQLException, InterruptedException {
					writelog2("+++ list NoAccess folders with children +++");
					setLastPathIdAvailable(true);
					Set<DBPathEntry> allRoots = getAllRoots();
					Set<Long> dontAccessRootIds = getDB().getInsertableRootIdSet();
					int count = list(dontAccessRootIds, minus(allRoots, dontAccessRootIds),
							"type=0 AND (status=2 OR parentid=0)", false);
					writelog2("+++ list NoAccess folders with children finished count=" + count + " +++");
					if (count > 0) {
						return false;
					} else {
						resetLastPathId();
						return true;
					}

				}
			},
			new ScheduleInsertable() {
				@Override public boolean isEol() throws SQLException, InterruptedException {
					writelog2("+++ list NoAccess folders without children +++");
					setLastPathIdAvailable(true);
					Set<DBPathEntry> allRoots = getAllRoots();
					Set<Long> dontAccessRootIds = getDB().getInsertableRootIdSet();
					int count = list(dontAccessRootIds, minus(allRoots, dontAccessRootIds),
							"type=0 AND (status=2 OR parentid=0)", true);
					writelog2("+++ list NoAccess folders without children finished count=" + count + " +++");
					if (count > 0) {
						return false;
					} else {
						resetLastPathId();
						return true;
					}

				}
			}
	};

	Schedule[] scheduleDontInsert = {
			new ScheduleDontInsert() {
				@Override public boolean isEol() throws SQLException, InterruptedException {
					writelog2("--- csum (1/2) ---");
					setLastPathIdAvailable(false);
					Set<DBPathEntry> allRoots = getAllRoots();
					String sql = "SELECT * FROM directory AS d1 WHERE ((type=1 OR type=3) AND status<>2)"
							+ " AND (size<0 OR (csum IS NULL AND EXISTS (SELECT * FROM directory AS d2"
							+ " WHERE (type=1 OR type=3) AND size=d1.size AND pathid<>d1.pathid)))"
							+ " AND EXISTS (SELECT * FROM directory AS d3 WHERE d3.pathid=d1.parentid)"
							+ " ORDER BY size DESC"
							;
					PreparedStatement ps = getDB().prepareStatement(sql);
					int count = csum(ps, allRoots);
					writelog2("--- csum (1/2) finished count=" + count + " ---");
					return true;
				}
			},
			new ScheduleDontInsert() {
				@Override public boolean isEol() throws SQLException, InterruptedException {
					setLastPathIdAvailable(false);
					Set<DBPathEntry> allRoots = getAllRoots();
					crawlEqualityUpdate(allRoots);
					return true;
				}
			},
			new ScheduleDontInsert() {
				@Override public boolean isEol() throws SQLException, InterruptedException {
					writelog2("--- csum (2/2) ---");
					setLastPathIdAvailable(true);
					Set<DBPathEntry> allRoots = getAllRoots();
					String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR type=3) AND status=2"
							+ " AND EXISTS (SELECT * FROM directory AS d2 WHERE d2.pathid=d1.parentid)"
							+ " AND pathid>? ORDER BY d1.pathid"
							;
					PreparedStatement ps = getDB().prepareStatement(sql);
					ps.setLong(1, getLastPathId());
					int count = csum(ps, allRoots);
					writelog2("--- csum (2/2) finished count=" + count + " ---");
					if (count>0) {
						return false;
					} else {
						resetLastPathId();
						return true;
					}
				}
			},
			new ScheduleDontInsert() {
				@Override public boolean isStartable() {
					if (getDB().getUpdateQueueSize(0) > 0) {
						return false;
					} else {
						return super.isStartable();
					}
				}
				@Override public boolean isEol() throws SQLException, InterruptedException {
					writelog2("--- touch ---");
					setLastPathIdAvailable(true);
					Set<DBPathEntry> allRoots = getAllRoots();
					String sql = "SELECT * FROM directory AS d1 WHERE ((type=0 AND status=0) OR type=1)"
							+ " AND EXISTS (SELECT * FROM directory AS d2 WHERE d2.pathid=d1.parentid AND d2.status=0)"
							+ " AND pathid>? ORDER BY d1.pathid"
							;
					PreparedStatement ps = getDB().prepareStatement(sql);
					ps.setLong(1, getLastPathId());
					int count = touch(ps, allRoots);
					writelog2("--- touch finished count=" + count + " ---");
					if (count>0) {
						return false;
					} else {
						resetLastPathId();
						return true;
					}
				}
			}
	};

	private static final int SCHEDULE_UPDATE_COUNT_THRESHOLD = UPDATE_QUEUE_SIZD_HIGH_THRESHOLD * 8 / 10;
	Schedule[] scheduleUpdate = {
			new ScheduleUpdate() {
				private int repeatCounter=0;
				@Override
				public boolean isEol() throws SQLException, InterruptedException {
					writelog2("*** refresh upperlower entries (1/2) ***");
					setLastPathIdAvailable(false);
					int c = getDB().refreshDirectUpperLower(getQueueLimit());
					writelog2("*** refresh upperlower entries (1/2) finished count=" + c + " ***");

					if (c>SCHEDULE_UPDATE_COUNT_THRESHOLD  && repeatCounter < 10) {
						repeatCounter++;
						return false;
					} else {
						repeatCounter = 0;
						return true;
					}
				}
			},
			new ScheduleUpdate() {
				private int repeatCounter=0;
				@Override
				public boolean isEol() throws SQLException, InterruptedException {
					writelog2("*** refresh upperlower entries (2/2) ***");
					setLastPathIdAvailable(false);
					int c = getDB().refreshIndirectUpperLower(getQueueLimit());
					writelog2("*** refresh upperlower entries (2/2) finished count=" + c + " ***");

					if (c>SCHEDULE_UPDATE_COUNT_THRESHOLD  && repeatCounter < 10) {
						repeatCounter++;
						return false;
					} else {
						repeatCounter = 0;
						return true;
					}
				}
			},
			new ScheduleUpdate() {
				@Override
				public boolean isEol() throws SQLException, InterruptedException {
					writelog2("*** unlist disabled extensions ***");
					setLastPathIdAvailable(false);
					int c = unlistDisabledExtensions();
					writelog2("*** unlist disabled extensions finished count=" + c + " ***");
					return true;
				}
			},
			new ScheduleUpdate() {
				@Override
				public boolean isEol() throws SQLException, InterruptedException {
					writelog2("*** orphanize orphan's children ***");
					setLastPathIdAvailable(false);
					int c = orphanizeOrphansChildren();
					writelog2("*** orphanize orphan's children finished count=" + c + " ***");
					return true;
				}
			},
			new ScheduleUpdate() {
				private int repeatCounter=0;
				@Override
				public boolean isEol() throws SQLException, InterruptedException {
					writelog2("*** refresh duplicate fields ***");
					setLastPathIdAvailable(false);
					int c = getDB().refreshDuplicateFields(getQueueLimit());
					writelog2("*** refresh duplicate fields finished count=" + c + " ***");

					if (c>SCHEDULE_UPDATE_COUNT_THRESHOLD  && repeatCounter < 10) {
						repeatCounter++;
						return false;
					} else {
						repeatCounter = 0;
						return true;
					}
				}
			},
			new ScheduleUpdate() {
				private int repeatCounter=0;
				@Override
				public boolean isEol() throws SQLException, InterruptedException {
					writelog2("*** refresh directory sizes ***");
					setLastPathIdAvailable(false);
					int c = getDB().refreshFolderSizes(getQueueLimit());
					writelog2("*** refresh directory sizes finished count=" + c + " ***");

					if (c>0  && repeatCounter < 10) {
						repeatCounter++;
						return false;
					} else {
						repeatCounter = 0;
						return true;
					}
				}
			},
			new ScheduleUpdate() {
				private int repeatCounter=0;
				@Override
				public boolean isEol() throws SQLException, InterruptedException {
					writelog2("*** cleanup orphans ***");
					setLastPathIdAvailable(false);
					int c = getDB().cleanupOrphans(getQueueLimit());
					writelog2("*** cleanup orphans finished count=" + c + " ***");

					if (c>SCHEDULE_UPDATE_COUNT_THRESHOLD  && repeatCounter < 10) {
						repeatCounter++;
						return false;
					} else {
						repeatCounter = 0;
						return true;
					}
				}
			},
			new ScheduleUpdate() {
				private int repeatCounter=0;
				@Override
				public boolean isEol() throws SQLException, InterruptedException {
					writelog2("*** cleanup equality orphans ***");
					setLastPathIdAvailable(false);
					int c = getDB().cleanupEqualityOrphans(getQueueLimit());
					writelog2("*** cleanup equality orphans finished count=" + c + " ***");

					if (c>SCHEDULE_UPDATE_COUNT_THRESHOLD  && repeatCounter < 10) {
						repeatCounter++;
						return false;
					} else {
						repeatCounter = 0;
						return true;
					}
				}
			},
			new ScheduleUpdate() {
				private int repeatCounter=0;
				@Override
				public boolean isEol() throws SQLException, InterruptedException {
					writelog2("*** cleanup upperlower orphans ***");
					setLastPathIdAvailable(false);
					int c = getDB().cleanupEqualityOrphans(getQueueLimit());
					writelog2("*** cleanup upperlower orphans finished count=" + c + " ***");

					if (c>SCHEDULE_UPDATE_COUNT_THRESHOLD  && repeatCounter < 10) {
						repeatCounter++;
						return false;
					} else {
						repeatCounter = 0;
						return true;
					}
				}
			}
	};

	private Set<DBPathEntry> getAllRoots() throws SQLException, InterruptedException {
		Set<DBPathEntry> result = new HashSet<DBPathEntry>();
		Statement stmt = getDB().createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM directory WHERE parentid=0");
		try {
			Dispatcher disp = getDB().getDispatcher();
			disp.setList(Dispatcher.NONE);
			disp.setCsum(Dispatcher.NONE);
			disp.setNoReturn(false);
			while (rs.next()) {
				DBPathEntry entry = getDB().rsToPathEntry(rs);
				result.add(entry);
			}
		} finally {
			rs.close();
			stmt.close();
		}
		return result;
	}

	private static Set<Long> getIdsFromEntries(Set<DBPathEntry> entries) {
		if (entries == null) { return null; }
		Set<Long> result = new HashSet<Long>();
		for (DBPathEntry entry: entries) {
			result.add(entry.getPathId());
		}
		return result;
	}

	private static String getSubSQLFromIds(Set<Long> ids) {
		List<String> s = new ArrayList<String>();
		if (ids != null) {
			for (Long i: ids) {
				s.add("rootid<>" + i);
			}
		}
		if (s.size() > 0) {
			return " AND (" + String.join(" AND ", s) + ") ";
		} else {
			return "";
		}
	}

	private int setAllCleanFoldersDirty(Set<Long> dontListRootIds) throws SQLException, InterruptedException {
		writelog2("*** set all clean folders dirty ***");
		String sql = "SELECT * FROM directory WHERE type=0 AND status=0" + getSubSQLFromIds(dontListRootIds);
		PreparedStatement ps = getDB().prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		int c2 = 0;
		try {
			while (rs.next()) {
				getDB().updateStatus(getDB().rsToPathEntry(rs), PathEntry.DIRTY);
				c2++;
			}
		} finally {
			rs.close();
			ps.close();
		}
		writelog2("*** set all clean folders dirty finished count=" + c2 + " ***");
		return c2;
	}

	private static Set<DBPathEntry> minus(Set<DBPathEntry> arg1, Set<Long> arg2) {
		Set<DBPathEntry> result = new HashSet<DBPathEntry>();
		for (DBPathEntry e: arg1) {
			if (! arg2.contains(e.getPathId())) {
				result.add(e);
			}
		}
		return result;
	}

	private String getArchiveExtSubSQL() {
		ArrayList<String> p = new ArrayList<String>();
		Map<String, Boolean> eal = getExtensionAvailabilityMap();
		for (String ext: PathEntryListerFactory.getExtensionList()) {
			Boolean v = eal.get(ext);
			if (v != null && v) {
				p.add("path LIKE '%." + ext + "'");
			}
		}
		String result;
		if (p.size() == 0) {
			result = "";
		} else {
			result = String.join(" OR ", p);
		}
		return result;
	}
}