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
		scheduleLayer1RoundRobinState = 0;
		scheduleLayer2RoundRobinState = 0;
		scheduleLayer3RoundRobinState = 0;
		try {
			while (true) {
				if (getDb().getDontInsertQueueSize() < DONT_INSERT_QUEUE_SIZE_LIMIT) {
					writelog2("--- scuedule layer 1 ---");
					scheduleLayer1(false);
				} else {
					writelog2("--- SKIP scuedule layer 1 ---");
				}

				if (getDb().getInsertableQueueSize() < INSERTABLE_QUEUE_SIZE_LIMIT) {
					writelog2("--- scuedule layer 2 ---");
					scheduleLayer2(false);
				} else {
					writelog2("--- SKIP scuedule layer 2 ---");
				}

				writelog2("--- scuedule layer 3 ---");
				scheduleLayer3(false);
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

	private Set<Long> getScheduleLayer1DontAccessRootIds(Set<DbPathEntry> allRoots)
			throws SQLException, InterruptedException {
		Set<Long> s = getDb().getDontInsertRootIdSet();
		Set<DbPathEntry> r = minus(allRoots, s);
		return InterSetOperation.or(getIdsFromEntries(getUnreachableRoots(r)), s);
	}

	private static int scheduleLayer1RoundRobinState = 0;
	private void scheduleLayer1(boolean doAllAtOnce) throws InterruptedException, SQLException, IOException {

		Set<DbPathEntry> allRoots = getAllRoots();
		Set<Long> allRootIds = getIdsFromEntries(allRoots);
		consumeSomeUpdateQueue();

		assert(scheduleLayer1RoundRobinState >= 0);
		assert(scheduleLayer1RoundRobinState <= 2);

		if (scheduleLayer1RoundRobinState == 0) {
			Set<Long> dontAccessRootIds = getIdsFromEntries(getUnreachableRoots(allRoots));
			if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_LIMIT
				|| InterSetOperation.include(dontAccessRootIds, allRootIds)
				) {
				writelog2("--- SKIP csum (1/2) ---");
			} else {
				writelog2("--- csum (1/2) ---");
				String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR (type=3 AND status<>2))"
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
				scheduleLayer1RoundRobinState++;
			}
		}

		if (scheduleLayer1RoundRobinState == 1) {
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
				scheduleLayer1RoundRobinState++;
			}
		}

		if (scheduleLayer1RoundRobinState == 2) {
			Set<Long> dontAccessRootIds = getScheduleLayer1DontAccessRootIds(allRoots);
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
				scheduleLayer1RoundRobinState++;
			}
		}

		if (!doAllAtOnce) {
			consumeSomeUpdateQueue();
			scheduleLayer1RoundRobinState++;
		}
		scheduleLayer1RoundRobinState = scheduleLayer1RoundRobinState % 3;
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

	private Set<Long> getScheduleLayer2DontAccessRootIds(Set<DbPathEntry> allRoots)
			throws SQLException, InterruptedException {
		Set<Long> s = getDb().getInsertableRootIdSet();
		Set<DbPathEntry> r = minus(allRoots, s);
		return InterSetOperation.or(getIdsFromEntries(getUnreachableRoots(r)), s);
	}

	private static int scheduleLayer2RoundRobinState = 0;
	private static int scheduleLayer2ListDirtyFoldersCounter = 0;
	private static Set<Long> scheduleLayer2ListDirtyFoldersFinished = null;
	private void scheduleLayer2(boolean doAllAtOnce) throws SQLException, InterruptedException, IOException {
		assert(scheduleLayer2RoundRobinState >= 0);
		assert(scheduleLayer2RoundRobinState <= 7);

		Set<DbPathEntry> allRoots = getAllRoots();
		Set<Long> allRootIds = getIdsFromEntries(allRoots);
		consumeSomeUpdateQueue();

		if (scheduleLayer2RoundRobinState == 0) {
			Set<Long> dontAccessRootIds = getScheduleLayer2DontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (1/8) +++");
			} else {
				writelog2("+++ list (1/8) +++");
				int count = list(dontAccessRootIds, "(type=1 AND (" + getArchiveExtSubSql() + ")) AND (status=1 OR status=2)", false);
				writelog2("+++ list (1/8) finished count=" + count + " +++");
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
			scheduleLayer2ListDirtyFoldersCounter = 0;
			scheduleLayer2ListDirtyFoldersFinished = null;
		}

		if (scheduleLayer2RoundRobinState == 1) {
			Set<Long> dontAccessRootIds = getScheduleLayer2DontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (2/8) +++");
			} else {
				writelog2("+++ list (2/8) +++");
				int count = list(dontAccessRootIds, "type=0 AND status=1", false);
				writelog2("+++ list (2/8) finished count=" + count + " +++");

				if (count>0 && scheduleLayer2ListDirtyFoldersCounter < 3) {
					scheduleLayer2RoundRobinState--;
					scheduleLayer2ListDirtyFoldersCounter++;
				} else if (count==0) {
					scheduleLayer2ListDirtyFoldersFinished = dontAccessRootIds;
				}
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
		}

		if (scheduleLayer2RoundRobinState == 2) {
			Set<Long> dontAccessRootIds = getScheduleLayer2DontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (3/8) +++");
			} else {
				writelog2("+++ list (3/8) +++");
				int count = list(dontAccessRootIds, "(type=1 AND (" + getArchiveExtSubSql() + ")) AND (status=1 OR status=2)", true);
				writelog2("+++ list (3/8) finished count=" + count + " +++");
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
			scheduleLayer2ListDirtyFoldersCounter = 0;
		}

		if (scheduleLayer2RoundRobinState == 3) {
			Set<Long> dontAccessRootIds = getScheduleLayer2DontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (4/8) +++");
			} else {
				writelog2("+++ list (4/8) +++");
				int count = list(dontAccessRootIds, "type=0 AND status=1", true, RELAXED_INSERTABLE_QUEUE_SIZE_LIMIT);
				writelog2("+++ list (4/8) finished count=" + count + " +++");

				if (count>0 && scheduleLayer2ListDirtyFoldersCounter < 3) {
					scheduleLayer2RoundRobinState--;
					scheduleLayer2ListDirtyFoldersCounter++;
				} else if (count==0 && scheduleLayer2ListDirtyFoldersFinished != null) {
					Set<Long> dontAccessRootIds2 =
							InterSetOperation.or(scheduleLayer2ListDirtyFoldersFinished, dontAccessRootIds);
					if (! InterSetOperation.include(dontAccessRootIds2, allRootIds)) {
						setAllCleanFoldersDirty(dontAccessRootIds2);
					}
				}
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
		}

		if (scheduleLayer2RoundRobinState == 4) {
			Set<Long> dontAccessRootIds = getScheduleLayer2DontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (5/8) +++");
			} else {
				writelog2("+++ list (5/8) +++");
				int count = list(dontAccessRootIds, "(type=3 AND (" + getArchiveExtSubSql() + ")) AND status=1", false);
				writelog2("+++ list (5/8) finished count=" + count + " +++");
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
		}

		if (scheduleLayer2RoundRobinState == 5) {
			Set<Long> dontAccessRootIds = getScheduleLayer2DontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (6/8) +++");
			} else {
				writelog2("+++ list (6/8) +++");
				int count = list(dontAccessRootIds, "type=0 AND (status=2 OR parentid=0)", false);
				writelog2("+++ list (6/8) finished count=" + count + " +++");
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
		}

		if (scheduleLayer2RoundRobinState == 6) {
			Set<Long> dontAccessRootIds = getScheduleLayer2DontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (7/8) +++");
			} else {
				writelog2("+++ list (7/8) +++");
				int count = list(dontAccessRootIds, "(type=3 AND (" + getArchiveExtSubSql() + ")) AND status=1", true);
				writelog2("+++ list (7/8) finished count=" + count + " +++");
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
		}

		if (scheduleLayer2RoundRobinState == 7) {
			Set<Long> dontAccessRootIds = getScheduleLayer2DontAccessRootIds(allRoots);
			if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
					|| InterSetOperation.include(dontAccessRootIds, allRootIds)
					) {
				writelog2("+++ SKIP list (8/8) +++");
			} else {
				writelog2("+++ list (8/8) +++");
				int count = list(dontAccessRootIds, "type=0 AND (status=2 OR parentid=0)", true);
				writelog2("+++ list (8/8) finished count=" + count + " +++");
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
		}

		if (!doAllAtOnce) {
			consumeSomeUpdateQueue();
			scheduleLayer2RoundRobinState++;
		}
		scheduleLayer2RoundRobinState = scheduleLayer2RoundRobinState % 8;
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

	private static int scheduleLayer3RoundRobinState = 0;
	private static int scheduleLayer3RepeatCounter = 0;
	private void scheduleLayer3(boolean doAllAtOnce) throws SQLException, InterruptedException {
		consumeSomeUpdateQueue();

		assert(scheduleLayer3RoundRobinState >= 0);
		assert(scheduleLayer3RoundRobinState <= 7);

		if (scheduleLayer3RoundRobinState == 0) {
			writelog2("*** refresh upperlower entries (1/2) ***");
			int c = getDb().refreshDirectUpperLower(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh upperlower entries (1/2) finished count=" + c + " ***");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			}
			scheduleLayer3RepeatCounter = 0;
		}

		if (scheduleLayer3RoundRobinState == 1) {
			writelog2("*** refresh upperlower entries (2/2) ***");
			int c = getDb().refreshIndirectUpperLower(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh upperlower entries (2/2) finished count=" + c + " ***");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			} else if (c>0  && scheduleLayer3RepeatCounter < 5) {
				scheduleLayer3RoundRobinState--;
				scheduleLayer3RepeatCounter++;
			}
		}

		if (scheduleLayer3RoundRobinState == 2) {
			unlistDisabledExtensions();

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			}
		}

		if (scheduleLayer3RoundRobinState == 3) {
			int c = orphanizeOrphansChildren();
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			} else if (c>0) {
				scheduleLayer3RoundRobinState--;
			}
		}

		if (scheduleLayer3RoundRobinState == 4) {
			writelog2("*** refresh duplicate fields ***");
			int c = getDb().refreshDuplicateFields(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh duplicate fields finished count=" + c + " ***");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			}
			scheduleLayer3RepeatCounter = 0;
		}

		if (scheduleLayer3RoundRobinState == 5) {
			writelog2("*** refresh directory sizes ***");
			int c = getDb().refreshFolderSizes(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh directory sizes finished count=" + c + " ***");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			} else if (c>0  && scheduleLayer3RepeatCounter < 10) {
				scheduleLayer3RoundRobinState--;
				scheduleLayer3RepeatCounter++;
			}
		}

		if (scheduleLayer3RoundRobinState == 6) {
			if (getDb().getUpdateQueueSize(1) > 0) {
				writelog2("*** SKIP cleanup orphans ***");
			} else {
				writelog2("*** cleanup orphans ***");
				int c = cleanupOrphans();
				writelog2("*** cleanup orphans finished count=" + c + " ***");
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			}
		}

		if (scheduleLayer3RoundRobinState == 7) {
			int c=0;
			if (getDb().getUpdateQueueSize(1) > 0) {
				getDb().consumeOneUpdateQueue();
				c++;
			}

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			} else if (c>0) {
				scheduleLayer3RoundRobinState--;
			}
		}

		if (!doAllAtOnce) {
			consumeSomeUpdateQueue();
			scheduleLayer3RoundRobinState++;
		}
		scheduleLayer3RoundRobinState = scheduleLayer3RoundRobinState % 8;
	}

	private static final int UPDATE_QUEUE_SIZE_LIMIT = 10000;
	private static final int INSERTABLE_QUEUE_SIZE_LIMIT = 100;
	private static final int RELAXED_INSERTABLE_QUEUE_SIZE_LIMIT = 1000;
	private static final int DONT_INSERT_QUEUE_SIZE_LIMIT = 10000;

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

	private Set<DbPathEntry> getUnreachableRoots(Set<DbPathEntry> allRoots) throws SQLException, InterruptedException {
		if (allRoots == null) { return null; }

		Set<DbPathEntry> result = new HashSet<DbPathEntry>();
		Dispatcher disp = getDb().getDispatcher();
		disp.setList(Dispatcher.NONE);
		disp.setCsum(Dispatcher.NONE);
		disp.setNoReturn(false);
		for (DbPathEntry entry: allRoots) {
			try {
				if (disp.dispatch(entry) == null) {
					result.add(entry);
				}
			} catch (IOException e) {
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

	private void unlistDisabledExtensions()
			throws SQLException, InterruptedException {
		writelog2("*** unlist disabled extensions ***");
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
			writelog2("*** unlist disabled extensions finished count=" + count + " ***");
		}
	}

	private void writelog2(final String message) {
		try {
			getDb().writelog2(message);
		} catch (SQLException e) {
			System.out.println(String.format("%s qL=- qC=- qS=- qT=- %s", new Date().toString(), message));
		}
	}

}
