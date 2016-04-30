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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.github.n_i_e.dirtreedb.LazyProxyDirTreeDb.Dispatcher;

public class StandardCrawler extends LazyAccessorThread {

	class RunnerThread extends LazyAccessorThread.RunnerThread {
		@Override
		public void run() {
			while (true) {
				try {
					writelog2("--- Crawler Main Loop ---");
					if (!IsWin32Idle.isWin32Idle()) {
						writelog2("--- Crawler Start Sleep ---");
						System.gc();
						while (!IsWin32Idle.isWin32Idle()) {
							Thread.sleep(1000);
						}
						writelog2("--- Crawler End Sleep ---");
					}
					writelog("--- Crawler Start Scenario (1/2) ---");
					startingHook1();
					getConf().registLowPriority();
					startingHook2();
					writelog("--- Crawler Start Scenario (2/2) ---");
					threadHook();
					StandardCrawler.this.run();
				} catch (InterruptedException e) {
					writelog("--- Crawler Interrupted ---");
				} catch (Throwable e) {
					writelog("Crawler Reached StandardCrawler bottom due to Exception: " + e.toString());
					writeError("Exception", "This may be a fatal trouble. Exiting.\n" + e.toString());
					e.printStackTrace();
					System.exit(1);
				} finally {
					writelog("--- Crawler End Scenario (1/2) ---");
					endingHook1();
					try {
						getConf().unregist();
					} catch (Throwable e) {
						writelog("Failed closeing DB file");
						writeError("Error", "Failed closeing DB file. This is may be a fatal trouble. Exiting.\n" + e.toString());
						e.printStackTrace();
						System.exit(1);
					}
					endingHook2();
					writelog("--- Crawler End Scenario (2/2) ---");
				}
			}
		}
	}

	public void start() {
		super.start(new RunnerThread());
	}

	public StandardCrawler(LazyAccessorThreadRunningConfig registry) {
		super(registry);
	}

	@Override
	public void run() throws Exception {
		scheduleDontInsertsRoundRobinState = 0;
		scheduleInsertablesRoundRobinState = 0;
		scheduleUpdatesRoundRobinState = 0;
		try {
			while (true) {
				if (getDb().getInsertableQueueSize() < INSERTABLE_QUEUE_SIZE_LIMIT) {
					writelog2("--- scuedule layer 1 ---");
					scheduleInsertables(false);
				} else {
					writelog2("--- SKIP scuedule layer 1 ---");
				}

				if (getDb().getDontInsertQueueSize() < RESTRICTED_DONT_INSERT_QUEUE_SIZE_LIMIT) {
					writelog2("--- scuedule layer 2 ---");
					scheduleDontInserts(false);
				} else {
					writelog2("--- SKIP scuedule layer 2 ---");
				}

				writelog2("--- scuedule layer 3 ---");
				scheduleUpdates(false);
			}
		} catch (SQLException e) {
			writeWarning("Warning", "Caught SQLException, trying to recover (this is usually OK)");
			writelog2("Crawler WARNING caught SQLException, trying to recover");
			e.printStackTrace();
			getDb().discardAllQueueItems();
			getDb().cleanupOrphans();
			getDb().consumeUpdateQueue();
		}
	}

	private Set<Long> getScheduleDontInsertsDontAccessRootIds(Set<DbPathEntry> allRoots)
			throws SQLException, InterruptedException {
		Set<Long> s = getDb().getDontInsertRootIdSet();
		Set<DbPathEntry> r = minus(allRoots, s);
		return InterSetOperation.or(getIdsFromEntries(getUnreachableRoots(r)), s);
	}

	private static int scheduleDontInsertsRoundRobinState = 0;
	private void scheduleDontInserts(boolean doAllAtOnce) throws InterruptedException, SQLException, IOException {

		Set<DbPathEntry> allRoots = getAllRoots();
		Set<Long> allRootIds = getIdsFromEntries(allRoots);
		consumeSomeUpdateQueue();

		assert(scheduleDontInsertsRoundRobinState >= 0);
		assert(scheduleDontInsertsRoundRobinState <= 2);

		if (scheduleDontInsertsRoundRobinState == 0) {
			Set<Long> dontAccessRootIds = getIdsFromEntries(getUnreachableRoots(allRoots));
			if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_LIMIT
				|| InterSetOperation.include(dontAccessRootIds, allRootIds)
				) {
				writelog2("--- SKIP csum (1/2) ---");
			} else {
				writelog2("--- csum (1/2) ---");
				String sql = "SELECT * FROM directory AS d1 WHERE ((type=1 OR type=3) AND status<>2)"
						+ getSubSqlFromIds(dontAccessRootIds)
						+ " AND (size<0 OR (csum IS NULL AND EXISTS (SELECT * FROM directory AS d2"
						+ " WHERE (type=1 OR type=3) AND size=d1.size AND pathid<>d1.pathid))) "
						+ " AND EXISTS (SELECT * FROM directory AS d3 WHERE d3.pathid=d1.parentid)"
						//+ "ORDER BY size DESC"
						;
				int count = csum(sql);
				writelog2("--- csum (1/2) finished count=" + count + " ---");
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleDontInsertsRoundRobinState++;
			}
		}

		if (scheduleDontInsertsRoundRobinState == 1) {
			Set<Long> dontAccessRootIds = getIdsFromEntries(getUnreachableRoots(allRoots));
			if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_LIMIT
				|| InterSetOperation.include(dontAccessRootIds, allRootIds)
				) {
				writelog2("--- SKIP equality ---");
			} else {
				crawlEqualityUpdate(dontAccessRootIds);
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleDontInsertsRoundRobinState++;
			}
		}

		if (scheduleDontInsertsRoundRobinState == 2) {
			Set<Long> dontAccessRootIds = getScheduleDontInsertsDontAccessRootIds(allRoots);
			if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_LIMIT
				|| InterSetOperation.include(dontAccessRootIds, allRootIds)
				) {
				writelog2("--- SKIP csum (2/2) ---");
			} else {
				writelog2("--- csum (2/2) ---");
				String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR type=3) AND (csum IS NULL OR status=2) "
						+ getSubSqlFromIds(dontAccessRootIds)
						+ " AND EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid AND status=0)"
						;

				int count = csum(sql);
				writelog2("--- csum (2/2) finished count=" + count + " ---");
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleDontInsertsRoundRobinState++;
			}
		}

		if (!doAllAtOnce) {
			consumeSomeUpdateQueue();
			scheduleDontInsertsRoundRobinState++;
		}
		scheduleDontInsertsRoundRobinState = scheduleDontInsertsRoundRobinState % 3;
	}

	private int csum(String sql) throws SQLException, InterruptedException, IOException {
		Statement stmt = getDb().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		writelog2("--- csum query finished ---");
		int count = 0;
		try {
			Dispatcher disp = getDb().getDispatcher();
			disp.setList(Dispatcher.NONE);
			disp.setCsum(Dispatcher.CSUM_FORCE);
			disp.setNoReturn(true);
			while (rs.next()) {
				DbPathEntry f = getDb().rsToPathEntry(rs);
				assert(f.isFile() || f.isCompressedFile());
				disp.dispatch(f);
				count++;
				if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_LIMIT
						|| getDb().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZE_LIMIT
						) {
					break;
				}
			}
		} finally {
			rs.close();
			stmt.close();
		}
		return count;
	}

	private int crawlEqualityUpdate(Set<Long> dontAccessRootIds) throws SQLException, InterruptedException, IOException {
		writelog2("--- equality ---");
		Dispatcher disp = getDb().getDispatcher();
		disp.setNoReturn(true);
		Statement stmt = getDb().createStatement();
		String sql = "SELECT * FROM equality ORDER BY datelasttested";
		ResultSet rs = stmt.executeQuery(sql);
		writelog2("--- equality query finished ---");
		int count = 0;
		try {
			while (rs.next()) {
				DbPathEntry p1 = getDb().getDbPathEntryByPathId(rs.getLong("pathid1"));
				if (dontAccessRootIds.contains(p1.getRootId())) { break; }
				DbPathEntry p2 = getDb().getDbPathEntryByPathId(rs.getLong("pathid2"));
				if (dontAccessRootIds.contains(p2.getRootId())) { break; }
				disp.checkEquality(p1, p2, disp.CHECKEQUALITY_UPDATE);
				count++;
				if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_LIMIT
						|| getDb().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZE_LIMIT
						) {
					break;
				}
			}
		} finally {
			rs.close();
		}
		writelog2("--- equality finished count=" + count + " ---");
		return count;
	}

	long debugBeaconMessageTimer = 0L;
	@Override
	public void threadHook() throws InterruptedException {
		long t = new Date().getTime();
		if (t - debugBeaconMessageTimer >= 10*60*1000) {
			debugBeaconMessageTimer = t;
			writelog2("beacon.");
		}

		super.threadHook();
		if (!IsWin32Idle.isWin32Idle()) {
			throw new InterruptedException("!! Windows busy now! get out!");
		}
	}

	public void consumeSomeUpdateQueue() throws InterruptedException, SQLException {
		while (getDb().getUpdateQueueSize(0) > 0) {
			getDb().consumeOneUpdateQueue();
		}
	}

	private static Set<DbPathEntry> minus(Set<DbPathEntry> arg1, Set<Long> arg2) {
		Set<DbPathEntry> result = new HashSet<DbPathEntry>();
		for (DbPathEntry e: arg1) {
			if (! arg2.contains(e.getPathId())) {
				result.add(e);
			}
		}
		return result;
	}

	private Set<Long> getScheduleInsertablesDontAccessRootIds(Set<DbPathEntry> allRoots)
			throws SQLException, InterruptedException {
		Set<Long> s = getDb().getInsertableRootIdSet();
		Set<DbPathEntry> r = minus(allRoots, s);
		return InterSetOperation.or(getIdsFromEntries(getUnreachableRoots(r)), s);
	}

	private static int scheduleInsertablesRoundRobinState = 0;
	private static int scheduleInsertablesListDirtyFoldersCounter = 0;
	private static Set<Long> scheduleInsertablesListDirtyFoldersFinished = null;
	private void scheduleInsertables(boolean doAllAtOnce) throws SQLException, InterruptedException, IOException {
		assert(scheduleInsertablesRoundRobinState >= 0);
		assert(scheduleInsertablesRoundRobinState <= 5);

		Set<DbPathEntry> allRoots = getAllRoots();
		Set<Long> allRootIds = getIdsFromEntries(allRoots);
		consumeSomeUpdateQueue();

		if (scheduleInsertablesRoundRobinState == 0) {
			Set<Long> dontAccessRootIds = getScheduleInsertablesDontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (1/6) +++");
			} else {
				writelog2("+++ list (1/6) +++");
				int count = list(dontAccessRootIds,
						"((type=1 OR type=3) AND (" + getArchiveExtSubSql() + ")) AND (status=1 OR status=2)",
						false);
				writelog2("+++ list (1/6) finished count=" + count + " +++");
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleInsertablesRoundRobinState++;
			}
			scheduleInsertablesListDirtyFoldersCounter = 0;
			scheduleInsertablesListDirtyFoldersFinished = null;
		}

		if (scheduleInsertablesRoundRobinState == 1) {
			Set<Long> dontAccessRootIds = getScheduleInsertablesDontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (2/6) +++");
			} else {
				writelog2("+++ list (2/6) +++");
				int count = list(dontAccessRootIds, "type=0 AND status=1", false);
				writelog2("+++ list (2/6) finished count=" + count + " +++");

				if (count>0 && scheduleInsertablesListDirtyFoldersCounter < 3) {
					scheduleInsertablesRoundRobinState--;
					scheduleInsertablesListDirtyFoldersCounter++;
				} else if (count==0) {
					scheduleInsertablesListDirtyFoldersFinished = dontAccessRootIds;
				}
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleInsertablesRoundRobinState++;
			}
		}

		if (scheduleInsertablesRoundRobinState == 2) {
			Set<Long> dontAccessRootIds = getScheduleInsertablesDontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (3/6) +++");
			} else {
				writelog2("+++ list (3/6) +++");
				int count = list(dontAccessRootIds,
						"((type=1 OR type=3) AND (" + getArchiveExtSubSql() + ")) AND (status=1 OR status=2)",
						true);
				writelog2("+++ list (3/6) finished count=" + count + " +++");
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleInsertablesRoundRobinState++;
			}
		}

		if (scheduleInsertablesRoundRobinState == 3) {
			Set<Long> dontAccessRootIds = getScheduleInsertablesDontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (4/6) +++");
			} else {
				writelog2("+++ list (4/6) +++");
				int count = list(dontAccessRootIds, "type=0 AND status=1", true, RELAXED_INSERTABLE_QUEUE_SIZE_LIMIT);
				writelog2("+++ list (4/6) finished count=" + count + " +++");

				if (count==0 && scheduleInsertablesListDirtyFoldersFinished != null) {
					Set<Long> dontAccessRootIds2 =
							InterSetOperation.or(scheduleInsertablesListDirtyFoldersFinished, dontAccessRootIds);
					if (! InterSetOperation.include(dontAccessRootIds2, allRootIds)) {
						setAllCleanFoldersDirty(dontAccessRootIds2);
					}
				}
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleInsertablesRoundRobinState++;
			}
		}

		if (scheduleInsertablesRoundRobinState == 4) {
			Set<Long> dontAccessRootIds = getScheduleInsertablesDontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (5/6) +++");
			} else {
				writelog2("+++ list (5/6) +++");
				int count = list(dontAccessRootIds, "type=0 AND (status=2 OR parentid=0)", false);
				writelog2("+++ list (5/6) finished count=" + count + " +++");
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleInsertablesRoundRobinState++;
			}
		}

		if (scheduleInsertablesRoundRobinState == 5) {
			Set<Long> dontAccessRootIds = getScheduleInsertablesDontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (6/6) +++");
			} else {
				writelog2("+++ list (6/6) +++");
				int count = list(dontAccessRootIds, "type=0 AND (status=2 OR parentid=0)", true);
				writelog2("+++ list (6/6) finished count=" + count + " +++");
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleInsertablesRoundRobinState++;
			}
		}

		if (!doAllAtOnce) {
			consumeSomeUpdateQueue();
			scheduleInsertablesRoundRobinState++;
		}
		scheduleInsertablesRoundRobinState = scheduleInsertablesRoundRobinState % 6;
	}

	private int setAllCleanFoldersDirty(Set<Long> dontListRootIds) throws SQLException, InterruptedException {
		writelog2("*** set all clean folders dirty ***");
		String sql = "SELECT * FROM directory WHERE type=0 AND status=0" + getSubSqlFromIds(dontListRootIds);
		PreparedStatement ps = getDb().prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		int c2 = 0;
		try {
			while (rs.next()) {
				getDb().updateStatus(getDb().rsToPathEntry(rs), PathEntry.DIRTY);
				c2++;
			}
		} finally {
			rs.close();
			ps.close();
		}
		writelog2("*** set all clean folders dirty finished count=" + c2 + " ***");
		return c2;
	}

	private int list(Set<Long> dontListRootIds,
			String typeStatusSubSql, boolean hasNoChild)
			throws SQLException, InterruptedException, IOException {
		return list(dontListRootIds, typeStatusSubSql, hasNoChild, INSERTABLE_QUEUE_SIZE_LIMIT);
	}

	private int list(Set<Long> dontListRootIds,
			String typeStatusSubSql, boolean hasNoChild, long insertableQueueSizeLimit)
			throws SQLException, InterruptedException, IOException {
		Dispatcher disp = getDb().getDispatcher();
		disp.setList(Dispatcher.LIST);
		disp.setCsum(Dispatcher.NONE);
		disp.setNoReturn(true);
		disp.setNoChildInDb(hasNoChild);

		String sql = "SELECT * FROM directory AS d1 WHERE "
				+ typeStatusSubSql
				+ getSubSqlFromIds(dontListRootIds)
				+ " AND " + (hasNoChild ? "NOT " : "") + "EXISTS (SELECT * FROM directory WHERE parentid=d1.pathid)"
				+ " AND (d1.parentid=0 OR EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid ))";
		writelog2(sql);
		Statement stmt = getDb().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		writelog2("+++ list query finished +++");
		int count = 0;
		try {
			while (rs.next()) {
				DbPathEntry f = getDb().rsToPathEntry(rs);
				Assertion.assertAssertionError(f.isFolder()
						|| ((f.isFile() || f.isCompressedFile()) && ArchiveListerFactory.isArchivable(f)),
						"!! CANNOT LIST THIS ENTRY: " + f.getType() + " at " + f.getPath());
				disp.dispatch(f);
				count++;
				if (getDb().getInsertableQueueSize() >= insertableQueueSizeLimit
						|| getDb().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZE_LIMIT
						) {
					return count;
				}
			}
		} finally {
			rs.close();
			stmt.close();
		}
		return count;
	}

	private final RunnableWithException2<SQLException, InterruptedException> consumeSomeUpdateQueueRunner =
			new RunnableWithException2<SQLException, InterruptedException>() {
		@Override
		public void run() throws InterruptedException, SQLException {
			consumeSomeUpdateQueue();
		}
	};

	private static int scheduleUpdatesRoundRobinState = 0;
	private static int scheduleUpdatesRepeatCounter = 0;
	private void scheduleUpdates(boolean doAllAtOnce) throws SQLException, InterruptedException {
		consumeSomeUpdateQueue();

		assert(scheduleUpdatesRoundRobinState >= 0);
		assert(scheduleUpdatesRoundRobinState <= 7);

		if (scheduleUpdatesRoundRobinState == 0) {
			writelog2("*** refresh upperlower entries (1/2) ***");
			int c = getDb().refreshDirectUpperLower(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh upperlower entries (1/2) finished count=" + c + " ***");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleUpdatesRoundRobinState++;
			}
			scheduleUpdatesRepeatCounter = 0;
		}

		if (scheduleUpdatesRoundRobinState == 1) {
			writelog2("*** refresh upperlower entries (2/2) ***");
			int c = getDb().refreshIndirectUpperLower(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh upperlower entries (2/2) finished count=" + c + " ***");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleUpdatesRoundRobinState++;
			} else if (c>0  && scheduleUpdatesRepeatCounter < 5) {
				scheduleUpdatesRoundRobinState--;
				scheduleUpdatesRepeatCounter++;
			}
		}

		if (scheduleUpdatesRoundRobinState == 2) {
			writelog2("*** unlist disabled extensions ***");
			int c = unlistDisabledExtensions();
			writelog2("*** unlist disabled extensions finished count=" + c + " ***");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleUpdatesRoundRobinState++;
			}
		}

		if (scheduleUpdatesRoundRobinState == 3) {
			writelog2("*** orphanize orphan's children ***");
			int c = orphanizeOrphansChildren();
			writelog2("*** orphanize orphan's children finished count=" + c + " ***");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleUpdatesRoundRobinState++;
			} else if (c>0) {
				scheduleUpdatesRoundRobinState--;
			}
		}

		if (scheduleUpdatesRoundRobinState == 4) {
			writelog2("*** refresh duplicate fields ***");
			int c = getDb().refreshDuplicateFields(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh duplicate fields finished count=" + c + " ***");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleUpdatesRoundRobinState++;
			}
			scheduleUpdatesRepeatCounter = 0;
		}

		if (scheduleUpdatesRoundRobinState == 5) {
			writelog2("*** refresh directory sizes ***");
			int c = getDb().refreshFolderSizes(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh directory sizes finished count=" + c + " ***");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleUpdatesRoundRobinState++;
			} else if (c>0  && scheduleUpdatesRepeatCounter < 10) {
				scheduleUpdatesRoundRobinState--;
				scheduleUpdatesRepeatCounter++;
			}
		}

		if (scheduleUpdatesRoundRobinState == 6) {
			if (getDb().getUpdateQueueSize(1) > 0) {
				writelog2("*** SKIP cleanup orphans ***");
			} else {
				writelog2("*** cleanup orphans ***");
				int c = cleanupOrphans();
				writelog2("*** cleanup orphans finished count=" + c + " ***");
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleUpdatesRoundRobinState++;
			}
		}

		if (scheduleUpdatesRoundRobinState == 7) {
			int c = getDb().getUpdateQueueSize(1);
			if (c>0) {
				while (c <= getDb().getUpdateQueueSize(1)) {
					getDb().consumeOneUpdateQueue();
				}
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleUpdatesRoundRobinState++;
			} else if (c>0) {
				scheduleUpdatesRoundRobinState--;
			}
		}

		if (!doAllAtOnce) {
			consumeSomeUpdateQueue();
			scheduleUpdatesRoundRobinState++;
		}
		scheduleUpdatesRoundRobinState = scheduleUpdatesRoundRobinState % 8;
	}

	private static final int UPDATE_QUEUE_SIZE_LIMIT = 10000;
	private static final int INSERTABLE_QUEUE_SIZE_LIMIT = 100;
	private static final int RELAXED_INSERTABLE_QUEUE_SIZE_LIMIT = 1000;
	private static final int DONT_INSERT_QUEUE_SIZE_LIMIT = 10000;
	private static final int RESTRICTED_DONT_INSERT_QUEUE_SIZE_LIMIT = 1000;

	private int cleanupOrphans() throws SQLException, InterruptedException {
		ProxyDirTreeDb.CleanupOrphansCallback cleanupOrphansCallbackRunner =
				new ProxyDirTreeDb.CleanupOrphansCallback() {
			@Override
			public boolean isEol() throws SQLException, InterruptedException {
				if (getDb().getUpdateQueueSize(1) >= UPDATE_QUEUE_SIZE_LIMIT) {
					return true;
				} else {
					return false;
				}
			}
		};

		return getDb().cleanupOrphans(cleanupOrphansCallbackRunner);
	}

	private int orphanizeOrphansChildren() throws SQLException, InterruptedException {
		String sql = "SELECT * FROM directory AS d1 WHERE EXISTS "
				+ "(SELECT * FROM directory AS d2 WHERE d1.parentid=d2.pathid AND d2.parentid<>0 "
				+ "AND NOT EXISTS (SELECT * FROM directory AS d3 WHERE d2.parentid=d3.pathid))";
		PreparedStatement ps = getDb().prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		try {
			int count = 0;
			while (rs.next()) {
				DbPathEntry entry = getDb().rsToPathEntry(rs);
				getDb().orphanizeLater(entry);
				count ++;
				if (getDb().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZE_LIMIT) {
					break;
				}
			}
			return count;
		} finally {
			rs.close();
			ps.close();
		}
	}

	private String getArchiveExtSubSql() {
		ArrayList<String> p = new ArrayList<String>();
		Map<String, Boolean> eal = getConf().getExtensionAvailabilityMap();
		for (String ext: ArchiveListerFactory.getExtensionList()) {
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

	private Set<DbPathEntry> getAllRoots() throws SQLException, InterruptedException {
		Set<DbPathEntry> result = new HashSet<DbPathEntry>();
		Statement stmt = getDb().createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM directory WHERE parentid=0");
		try {
			Dispatcher disp = getDb().getDispatcher();
			disp.setList(Dispatcher.NONE);
			disp.setCsum(Dispatcher.NONE);
			disp.setNoReturn(false);
			while (rs.next()) {
				DbPathEntry entry = getDb().rsToPathEntry(rs);
				result.add(entry);
			}
		} finally {
			rs.close();
			stmt.close();
		}
		return result;
	}

	private Set<DbPathEntry> getUnreachableRoots(Set<DbPathEntry> allRoots) {
		if (allRoots == null) { return null; }

		Set<DbPathEntry> result = new HashSet<DbPathEntry>();
		for (DbPathEntry entry: allRoots) {
			File f = new File(entry.getPath());
			if (!f.exists()) {
				result.add(entry);
			}
		}
		return result;
	}

	private static Set<Long> getIdsFromEntries(Set<DbPathEntry> entries) {
		if (entries == null) { return null; }
		Set<Long> result = new HashSet<Long>();
		for (DbPathEntry entry: entries) {
			result.add(entry.getPathId());
		}
		return result;
	}

	private static String getSubSqlFromIds(Set<Long> ids) {
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

	private int unlistDisabledExtensions()
			throws SQLException, InterruptedException {
		ArrayList<String> ext = new ArrayList<String>();
		Map<String, Boolean> eal = getConf().getExtensionAvailabilityMap();
		for (Entry<String, Boolean> kv: eal.entrySet()) {
			if (! kv.getValue()) {
				ext.add("path LIKE '%." + kv.getKey() + "'");
			}
		}
		String dontArchiveExtSubSql;
		if (ext.size() == 0) {
			writelog2("*** SKIP unlist disabled extensions (nothing to unlist) ***");
			return 0;
		} else {
			dontArchiveExtSubSql = "AND (" + String.join(" OR ", ext) + ")";
			String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR type=3) " + dontArchiveExtSubSql
					+ " AND EXISTS (SELECT * FROM directory AS d2 WHERE d2.parentid=d1.pathid)"
					+ " AND EXISTS (SELECT * FROM directory AS d3 WHERE d1.parentid=d3.pathid)";
			Statement stmt = getDb().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			int count = 0;
			try {
				while (rs.next()) {
					DbPathEntry f = getDb().rsToPathEntry(rs);
					Assertion.assertAssertionError(f.isFile() || f.isCompressedFile());
					getDb().updateStatus(f, PathEntry.DIRTY);
					getDb().orphanizeChildren(f);
					count++;
				}
			} finally {
				rs.close();
				stmt.close();
			}
			return count;
		}
	}

	private void writelog2(final String message) {
		try {
			getDb().writelog2(message);
		} catch (Exception e) {
			System.out.println(String.format("%s qL=- qC=- qS=- qT=- %s", new Date().toString(), message));
		}
	}

}
