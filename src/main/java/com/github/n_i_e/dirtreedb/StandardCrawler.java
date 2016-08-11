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

	private static final int UPDATE_QUEUE_SIZE_LIMIT = 10000;
	private static final int INSERTABLE_QUEUE_LOW_THRESHOLD = 50;
	private static final int INSERTABLE_QUEUE_HIGH_THRESHOLD = 100;
	private static final int RELAXED_INSERTABLE_QUEUE_SIZE_HIGH_THRESHOLD = 1000;
	private static final int DONT_INSERT_QUEUE_SIZE_LOW_THRESHOLD = 9000;
	private static final int DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD = 10000;

	@Override
	public void run() throws Exception {
		ScheduleDontInserts scheduleDontInserts = new ScheduleDontInserts();
		scheduleDontInserts.init();
		ScheduleInsertables scheduleInsertables = new ScheduleInsertables();
		scheduleInsertables.init();
		ScheduleUpdates scheduleUpdates = new ScheduleUpdates();
		scheduleUpdates.init();
		try {
			while (true) {
				if (getDb().getInsertableQueueSize() < INSERTABLE_QUEUE_LOW_THRESHOLD) {
					writelog2("--- scuedule layer 1 ---");
					scheduleInsertables.schedule(false);
				} else {
					writelog2("--- SKIP scuedule layer 1 ---");
				}

				if (getDb().getDontInsertQueueSize() < DONT_INSERT_QUEUE_SIZE_LOW_THRESHOLD) {
					writelog2("--- scuedule layer 2 ---");
					scheduleDontInserts.schedule(false);
				} else {
					writelog2("--- SKIP scuedule layer 2 ---");
				}

				writelog2("--- scuedule layer 3 ---");
				scheduleUpdates.schedule(false);
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

	private interface Scheduler {
		abstract void init();
		abstract void schedule(boolean doAllAtOnce) throws SQLException, InterruptedException, IOException;
	}

	private class ScheduleInsertables implements Scheduler {
		private int roundrobinState = 0;
		private int repeatCounter = 0;
		private long lastPathId = -1;
		private Set<Long> listFoldersWithChildrenFinished = null;

		@Override
		public void init() {
			roundrobinState = 0;
			repeatCounter = 0;
			lastPathId = -1;
			listFoldersWithChildrenFinished = null;
		}

		@Override
		public void schedule(boolean doAllAtOnce) throws SQLException, InterruptedException, IOException {
			assert(roundrobinState >= 0);
			assert(roundrobinState <= 5);

			Set<DbPathEntry> allRoots = getAllRoots();
			Set<Long> allRootIds = getIdsFromEntries(allRoots);
			Set<Long> dontAccessRootIds = getDb().getInsertableRootIdSet();
			consumeSomeUpdateQueue();

			if (roundrobinState == 0) {
				if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_LOW_THRESHOLD
						|| InterSetOperation.include(dontAccessRootIds, allRootIds)
						) {
					writelog2("+++ SKIP list folders with children +++");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					writelog2("+++ list folders with children +++");
					int count = list(dontAccessRootIds,  minus(allRoots, dontAccessRootIds),
							"type=0 AND status=1", false, false, INSERTABLE_QUEUE_HIGH_THRESHOLD);
					writelog2("+++ list folders with children finished count=" + count + " +++");

					if (count>0 && repeatCounter < 3) {
						roundrobinState--;
						repeatCounter++;
					} else {
						if (count==0) {
							listFoldersWithChildrenFinished = dontAccessRootIds;
						}
						repeatCounter = 0;
					}
				}
				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 1) {
				if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_LOW_THRESHOLD
						|| InterSetOperation.include(dontAccessRootIds, allRootIds)
						) {
					writelog2("+++ SKIP list folders without children +++");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					writelog2("+++ list folders without children +++");
					int count = list(dontAccessRootIds,  minus(allRoots, dontAccessRootIds),
							"type=0 AND status=1", true, false, RELAXED_INSERTABLE_QUEUE_SIZE_HIGH_THRESHOLD);
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
					}
				}

				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 2) {
				if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_LOW_THRESHOLD
						|| InterSetOperation.include(dontAccessRootIds, allRootIds)
						) {
					writelog2("+++ SKIP list files with children +++");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					writelog2("+++ list files with children +++");
					int count = list(dontAccessRootIds, minus(allRoots, dontAccessRootIds),
							"((type=1 OR type=3) AND (" + getArchiveExtSubSql() + ")) AND status=1",
							false, false, INSERTABLE_QUEUE_HIGH_THRESHOLD);
					writelog2("+++ list files with children finished count=" + count + " +++");
				}
				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 3) {
				if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_LOW_THRESHOLD
						|| InterSetOperation.include(dontAccessRootIds, allRootIds)
						) {
					writelog2("+++ SKIP list files without children +++");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					writelog2("+++ list files without children +++");
					int count = list(dontAccessRootIds, minus(allRoots, dontAccessRootIds),
							"((type=1 OR type=3) AND (" + getArchiveExtSubSql() + ")) AND status=1",
							true, false, INSERTABLE_QUEUE_HIGH_THRESHOLD);
					writelog2("+++ list files without children finished count=" + count + " +++");
				}
				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 4) {
				if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_LOW_THRESHOLD
						|| InterSetOperation.include(dontAccessRootIds, allRootIds)
						) {
					writelog2("+++ SKIP list NoAccess folders with children +++");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					writelog2("+++ list NoAccess folders with children +++");
					int count = list(dontAccessRootIds, minus(allRoots, dontAccessRootIds),
							"type=0 AND (status=2 OR parentid=0)", false, true, INSERTABLE_QUEUE_HIGH_THRESHOLD);
					writelog2("+++ list NoAccess folders with children finished count=" + count + " +++");
					if (count > 0) {
						roundrobinState --;
					} else {
						lastPathId = -1;
					}
				}

				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 5) {
				if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_LOW_THRESHOLD
						|| InterSetOperation.include(dontAccessRootIds, allRootIds)
						) {
					writelog2("+++ SKIP list NoAccess folders without children +++");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					writelog2("+++ list NoAccess folders without children +++");
					int count = list(dontAccessRootIds, minus(allRoots, dontAccessRootIds),
							"type=0 AND (status=2 OR parentid=0)", true, true, RELAXED_INSERTABLE_QUEUE_SIZE_HIGH_THRESHOLD);
					writelog2("+++ list NoAccess folders without children finished count=" + count + " +++");
					if (count > 0) {
						roundrobinState --;
					} else {
						lastPathId = -1;
					}
				}

				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (!doAllAtOnce) {
				consumeSomeUpdateQueue();
				roundrobinState++;
			}
			roundrobinState = roundrobinState % 6;
		}

		private int list(Set<Long> dontListRootIds, Set<DbPathEntry> rootmap,
				String typeStatusSubSql, boolean hasNoChild, boolean useLastPathId, long insertableQueueSizeLimit)
				throws SQLException, InterruptedException, IOException {
			Dispatcher disp = getDb().getDispatcher();
			disp.setList(Dispatcher.LIST);
			disp.setCsum(Dispatcher.NONE);
			disp.setNoReturn(true);
			disp.setNoChildInDb(hasNoChild);
			disp.setReachableRoots(rootmap);

			String sql = "SELECT * FROM directory AS d1 WHERE "
					+ typeStatusSubSql
					+ getSubSqlFromIds(dontListRootIds)
					+ " AND " + (hasNoChild ? "NOT " : "") + "EXISTS (SELECT * FROM directory WHERE parentid=d1.pathid)"
					+ " AND (d1.parentid=0 OR EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid ))"
					+ (useLastPathId ? " AND d1.pathid>? ORDER BY d1.pathid" : "")
					;
			//writelog2(sql);
			PreparedStatement ps = getDb().prepareStatement(sql);
			if (useLastPathId) {
				//writelog2(String.valueOf(lastPathId));
				ps.setLong(1, lastPathId);
			}
			ResultSet rs = ps.executeQuery();
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
					if (useLastPathId) {
						lastPathId = f.getPathId();
					}
					if (getDb().getInsertableQueueSize() >= insertableQueueSizeLimit
							|| getDb().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZE_LIMIT
							) {
						return count;
					}
				}
			} finally {
				rs.close();
				ps.close();
			}
			return count;
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

	}

	private class ScheduleDontInserts implements Scheduler {
		private int roundrobinState = 0;
		private long lastPathId = -1;

		@Override
		public void init() {
			roundrobinState = 0;
			lastPathId = -1;
		}

		@Override
		public void schedule(boolean doAllAtOnce) throws SQLException, InterruptedException, IOException {
			Set<DbPathEntry> allRoots = getAllRoots();
			consumeSomeUpdateQueue();

			assert(roundrobinState >= 0);
			assert(roundrobinState <= 4);

			if (roundrobinState == 0) {
				if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD) {
					writelog2("--- SKIP csum (1/2) ---");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					writelog2("--- csum (1/2) ---");
					String sql = "SELECT * FROM directory AS d1 WHERE ((type=1 OR type=3) AND status<>2)"
							+ " AND (size<0 OR (csum IS NULL AND EXISTS (SELECT * FROM directory AS d2"
							+ " WHERE (type=1 OR type=3) AND size=d1.size AND pathid<>d1.pathid)))"
							+ " AND EXISTS (SELECT * FROM directory AS d3 WHERE d3.pathid=d1.parentid)"
							+ " ORDER BY size DESC"
							;
					PreparedStatement ps = getDb().prepareStatement(sql);
					int count = csum(ps, allRoots, false);
					writelog2("--- csum (1/2) finished count=" + count + " ---");
				}
				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 1) {
				if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD) {
					writelog2("--- SKIP equality ---");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					crawlEqualityUpdate(allRoots);
				}
				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 2) {
				if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD) {
					writelog2("--- SKIP csum (2/2) ---");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					writelog2("--- csum (2/2) ---");
					String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR type=3) AND status=2"
							+ " AND EXISTS (SELECT * FROM directory AS d2 WHERE d2.pathid=d1.parentid)"
							+ " AND pathid>? ORDER BY d1.pathid"
							;
					PreparedStatement ps = getDb().prepareStatement(sql);
					ps.setLong(1, lastPathId);
					int count = csum(ps, allRoots, true);
					writelog2("--- csum (2/2) finished count=" + count + " ---");
					if (count>0) {
						roundrobinState--;
					} else {
						lastPathId = -1;
					}
				}
				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 4) {
				if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD) {
					writelog2("--- SKIP touch ---");
					if (!doAllAtOnce) { roundrobinState--; }
				} else {
					writelog2("--- touch ---");
					String sql = "SELECT * FROM directory AS d1 WHERE ((type=0 AND status=0) OR type=1)"
							+ " AND EXISTS (SELECT * FROM directory AS d2 WHERE d2.pathid=d1.parentid AND d2.status=0)"
							+ " AND pathid>? ORDER BY d1.pathid"
							;
					PreparedStatement ps = getDb().prepareStatement(sql);
					ps.setLong(1, lastPathId);
					int count = touch(ps, allRoots);
					writelog2("--- touch finished count=" + count + " ---");
					if (count>0) {
						roundrobinState--;
					} else {
						lastPathId = -1;
					}
				}
				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (!doAllAtOnce) {
				consumeSomeUpdateQueue();
				roundrobinState++;
			}
			roundrobinState = roundrobinState % 5;
		}

		private int csum(PreparedStatement ps, Set<DbPathEntry> reachableRoots, boolean useLastPathId)
				throws SQLException, InterruptedException, IOException {
			ResultSet rs = ps.executeQuery();
			writelog2("--- csum query finished ---");
			int count = 0;
			try {
				Dispatcher disp = getDb().getDispatcher();
				disp.setList(Dispatcher.NONE);
				disp.setCsum(Dispatcher.CSUM_FORCE);
				disp.setNoReturn(true);
				disp.setReachableRoots(reachableRoots);
				while (rs.next()) {
					DbPathEntry f = getDb().rsToPathEntry(rs);
					assert(f.isFile() || f.isCompressedFile());
					disp.dispatch(f);
					count++;
					if (useLastPathId) {
						lastPathId = f.getPathId();
					}
					if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD
							|| getDb().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZE_LIMIT
							) {
						break;
					}
				}
			} finally {
				rs.close();
				ps.close();
			}
			return count;
		}

		private int touch(PreparedStatement ps, Set<DbPathEntry> reachableRoots)
				throws SQLException, InterruptedException, IOException {
			ResultSet rs = ps.executeQuery();
			writelog2("--- touch query finished ---");
			int count = 0;
			try {
				Dispatcher disp = getDb().getDispatcher();
				disp.setList(Dispatcher.NONE);
				disp.setCsum(Dispatcher.NONE);
				disp.setNoReturn(true);
				disp.setReachableRoots(reachableRoots);
				while (rs.next()) {
					DbPathEntry f = getDb().rsToPathEntry(rs);
					assert(f.isFolder() || f.isFile());
					disp.dispatch(f);
					count++;
					lastPathId = f.getPathId();
					if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD
							|| getDb().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZE_LIMIT
							) {
						break;
					}
				}
			} finally {
				rs.close();
				ps.close();
			}
			return count;
		}

		private int crawlEqualityUpdate(Set<DbPathEntry> rootmap) throws SQLException, InterruptedException, IOException {
			writelog2("--- equality ---");
			Dispatcher disp = getDb().getDispatcher();
			disp.setNoReturn(true);
			disp.setReachableRoots(rootmap);
			Statement stmt = getDb().createStatement();
			String sql = "SELECT * FROM equality ORDER BY datelasttested";
			ResultSet rs = stmt.executeQuery(sql);
			writelog2("--- equality query finished ---");
			int count = 0;
			try {
				while (rs.next()) {
					DbPathEntry p1 = getDb().getDbPathEntryByPathId(rs.getLong("pathid1"));
					DbPathEntry p2 = getDb().getDbPathEntryByPathId(rs.getLong("pathid2"));
					disp.checkEquality(p1, p2, disp.CHECKEQUALITY_UPDATE);
					count++;
					if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_HIGH_THRESHOLD
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

	}

	private class ScheduleUpdates implements Scheduler {
		private int roundrobinState = 0;
		private int repeatCounter = 0;
		private final RunnableWithException2<SQLException, InterruptedException> consumeSomeUpdateQueueRunner =
				new RunnableWithException2<SQLException, InterruptedException>() {
			@Override
			public void run() throws InterruptedException, SQLException {
				consumeSomeUpdateQueue();
			}
		};

		@Override
		public void init() {
			roundrobinState = 0;
			repeatCounter = 0;
		}

		@Override
		public void schedule(boolean doAllAtOnce) throws SQLException, InterruptedException {
			consumeSomeUpdateQueue();

			assert(roundrobinState >= 0);
			assert(roundrobinState <= 7);

			if (roundrobinState == 0) {
				writelog2("*** refresh upperlower entries (1/2) ***");
				int c = getDb().refreshDirectUpperLower(consumeSomeUpdateQueueRunner);
				writelog2("*** refresh upperlower entries (1/2) finished count=" + c + " ***");

				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 1) {
				writelog2("*** refresh upperlower entries (2/2) ***");
				int c = getDb().refreshIndirectUpperLower(consumeSomeUpdateQueueRunner);
				writelog2("*** refresh upperlower entries (2/2) finished count=" + c + " ***");
				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				} else if (c>0  && repeatCounter < 5) {
					roundrobinState--;
					repeatCounter++;
				} else {
					repeatCounter = 0;
				}
			}

			if (roundrobinState == 2) {
				writelog2("*** unlist disabled extensions ***");
				int c = unlistDisabledExtensions();
				writelog2("*** unlist disabled extensions finished count=" + c + " ***");

				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 3) {
				writelog2("*** orphanize orphan's children ***");
				int c = orphanizeOrphansChildren();
				writelog2("*** orphanize orphan's children finished count=" + c + " ***");
				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				} else if (c>0) {
					roundrobinState--;
				}
			}

			if (roundrobinState == 4) {
				writelog2("*** refresh duplicate fields ***");
				int c = getDb().refreshDuplicateFields(consumeSomeUpdateQueueRunner);
				writelog2("*** refresh duplicate fields finished count=" + c + " ***");

				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
				repeatCounter = 0;
			}

			if (roundrobinState == 5) {
				writelog2("*** refresh directory sizes ***");
				int c = getDb().refreshFolderSizes(consumeSomeUpdateQueueRunner);
				writelog2("*** refresh directory sizes finished count=" + c + " ***");

				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				} else if (c>0  && repeatCounter < 10) {
					roundrobinState--;
					repeatCounter++;
				} else {
					repeatCounter = 0;
				}
			}

			if (roundrobinState == 6) {
				if (getDb().getUpdateQueueSize(1) > 0) {
					writelog2("*** SKIP cleanup orphans ***");
				} else {
					writelog2("*** cleanup orphans ***");
					int c = cleanupOrphans();
					writelog2("*** cleanup orphans finished count=" + c + " ***");
				}

				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				}
			}

			if (roundrobinState == 7) {
				int c = getDb().getUpdateQueueSize(1);
				if (c>0) {
					while (c <= getDb().getUpdateQueueSize(1)) {
						getDb().consumeOneUpdateQueue();
					}
				}

				if (doAllAtOnce) {
					consumeSomeUpdateQueue();
					roundrobinState++;
				} else if (c>0) {
					roundrobinState--;
				}
			}

			if (!doAllAtOnce) {
				consumeSomeUpdateQueue();
				roundrobinState++;
			}
			roundrobinState = roundrobinState % 8;
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

		private final ProxyDirTreeDb.CleanupOrphansCallback cleanupOrphansCallbackRunner =
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

		private int cleanupOrphans() throws SQLException, InterruptedException {
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

	private void writelog2(final String message) {
		try {
			getDb().writelog2(message);
		} catch (Exception e) {
			System.out.println(String.format("%s qL=- qC=- qS=- qT=- %s", new Date().toString(), message));
		}
	}

}
