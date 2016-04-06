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
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.github.n_i_e.dirtreedb.LazyProxyDirTreeDb.Dispatcher;
import com.github.n_i_e.dirtreedb.ProxyDirTreeDb.CleanupOrphansCallback;

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

	// "d1.*, d2.*" notation does not work for H2 database
	private static final String d1d2SubSql = "d1.pathid AS d1_pathid, d1.parentid AS d1_parentid, "
			+ "d1.rootid AS d1_rootid, d1.datelastmodified AS d1_datelastmodified, "
			+ "d1.size AS d1_size, d1.compressedsize AS d1_compressedsize, d1.csum AS d1_csum, "
			+ "d1.path AS d1_path, d1.type AS d1_type, d1.status AS d1_status, "
			+ "d1.duplicate AS d1_duplicate, d1.dedupablesize AS d1_dedupablesize, "
			+ "d2.pathid AS d2_pathid, d2.parentid AS d2_parentid, "
			+ "d2.rootid AS d2_rootid, d2.datelastmodified AS d2_datelastmodified, "
			+ "d2.size AS d2_size, d2.compressedsize AS d2_compressedsize, d2.csum AS d2_csum, "
			+ "d2.path AS d2_path, d2.type AS d2_type, d2.status AS d2_status, "
			+ "d2.duplicate AS d2_duplicate, d2.dedupablesize AS d2_dedupablesize";

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

				if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_LIMIT
						&& getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT) {
					writelog2("--- scuedule layer 3 ---");
					scheduleLayer3(false,
							new ProxyDirTreeDb.CleanupOrphansCallback() {
						public boolean isEol() throws SQLException, InterruptedException {
							if (getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_LIMIT
									&& getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT) {
								return false;
							} else {
								return true;
							}
						}
					});
				}
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

	private static int scheduleLayer1RoundRobinState = 0;
	private void scheduleLayer1(boolean doAllAtOnce) throws InterruptedException, SQLException, IOException {
		consumeSomeUpdateQueue();

		boolean dontCrawlEquality = false;
		String dontCheckUpdateRootIdsSubSql = "";

		assert(scheduleLayer1RoundRobinState >= 0);
		assert(scheduleLayer1RoundRobinState <= 2);

		if (scheduleLayer1RoundRobinState == 0) {
			writelog2("--- csum (1/2) ---");
			String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR (type=3 AND status<>2))"
					+ dontCheckUpdateRootIdsSubSql
					+ " AND (size<0 OR (csum IS NULL AND EXISTS (SELECT * FROM directory AS d2"
					+ " WHERE (type=1 OR type=3) AND size=d1.size AND pathid<>d1.pathid))) "
					+ " AND EXISTS (SELECT * FROM directory AS d3 WHERE d3.pathid=d1.parentid)"
					//+ "ORDER BY size DESC"
					;
			Statement stmt = getDb().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			writelog2("--- csum (1/2) query finished ---");
			int count = 0;
			try {
				Dispatcher disp = getDb().getDispatcher();
				disp.setList(Dispatcher.NONE);
				disp.setCsum(Dispatcher.CSUM);
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
			writelog2("--- csum (1/2) finished count=" + count + " ---");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer1RoundRobinState++;
			}
		}

		if (scheduleLayer1RoundRobinState == 1) {
			if (!dontCrawlEquality) {
				crawlEqualityUpdate();
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer1RoundRobinState++;
			}
		}

		if (scheduleLayer1RoundRobinState == 2) {
			consumeDontInsertQueue();
			writelog2("--- csum (2/2) ---");
			String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR type=3) AND (csum IS NULL OR status=2) "
					+ dontCheckUpdateRootIdsSubSql
					+ " AND EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid AND status=0)"
					;
			Statement stmt = getDb().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			writelog2("--- csum (2/2) query finished ---");
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
			writelog2("--- csum (2/2) finished count=" + count + " ---");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer1RoundRobinState++;
			}
		}

		if (!doAllAtOnce) {
			scheduleLayer1RoundRobinState++;
		}
		scheduleLayer1RoundRobinState = scheduleLayer1RoundRobinState % 3;
	}

	private int crawlEqualityNew(boolean skipNoAccess, List<Long> dontListRootIds)
			throws SQLException, InterruptedException, IOException {
		int count1 = 0;
		Statement stmt = getDb().createStatement();
		try {
			String sql1 = "SELECT size, csum FROM directory WHERE csum IS NOT NULL AND duplicate>=1 "
					+ (skipNoAccess ? "AND status<>2 " : "")
					+ "AND EXISTS (SELECT * FROM (SELECT COUNT(*) AS c FROM equality "
					+ "WHERE pathid1=pathid OR pathid2=pathid) WHERE c<duplicate) GROUP BY size, csum"
					;
			writelog2(sql1);
			ResultSet rs1 = stmt.executeQuery(sql1);
			writelog2("+++ equality (new) query finished +++");
			try {
				Dispatcher disp = getDb().getDispatcher();
				disp.setNoReturn(true);
				while (rs1.next()) {
					String sql2 = "SELECT " + d1d2SubSql + " FROM directory AS d1, directory AS d2 "
							+ "WHERE (d1.type=1 OR d1.type=3) AND (d2.type=1 OR d2.type=3) "
							+ getDontListRootIdsSubSql_D1D2(dontListRootIds)
							+ "AND d1.size=? AND d1.csum=? AND d2.size=? AND d2.csum=? "
							+ "AND d1.pathid>d2.pathid "
							+ (skipNoAccess ? "AND d1.status<>2 AND d2.status<>2 " : "")
							+ "AND NOT EXISTS (SELECT * FROM equality WHERE d1.pathid=pathid1 AND d2.pathid=pathid2)";
					PreparedStatement ps2 = getDb().prepareStatement(sql2);
					ps2.setLong(1, rs1.getLong("size"));
					ps2.setInt (2, rs1.getInt("csum"));
					ps2.setLong(3, rs1.getLong("size"));
					ps2.setInt (4, rs1.getInt("csum"));
					ResultSet rs2 = ps2.executeQuery();
					int count2 = 0;
					try {
						while (rs2.next()) {
							DbPathEntry p1 = getDb().rsToPathEntry(rs2, "d1_");
							DbPathEntry p2 = getDb().rsToPathEntry(rs2, "d2_");
							disp.checkEquality(p1, p2, disp.CHECKEQUALITY_INSERT);
							//cleanupDbAndListIfRecommended();
							consumeSomeUpdateQueue();
							count1++;
							count2++;
						}
					} finally {
						rs2.close();
					}
					if (count2>0) {
						writelog2("+++ equality (new) "+rs1.getLong("size")+" "+rs1.getLong("csum")+" "+count2+" +++");
					}
				}
			} finally {
				rs1.close();
			}
		} finally {
			stmt.close();
		}
		return count1;
	}

	private static String getDontListRootIdsSubSql_D1D2(List<Long> dontListRootIds) {
		String dontListRootIdsSubSql;
		ArrayList<String> s = new ArrayList<String>();
		if (dontListRootIds != null) {
			for (Long i: dontListRootIds) {
				s.add("d1.rootid<>" + i);
				s.add("d2.rootid<>" + i);
			}
		}
		if (s.size() > 0) {
			dontListRootIdsSubSql = " AND (" + String.join(" AND ", s) + ") ";
		} else {
			dontListRootIdsSubSql = "";
		}
		return dontListRootIdsSubSql;
	}

	private int crawlEqualityUpdate() throws SQLException, InterruptedException, IOException {
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
				DbPathEntry p2 = getDb().getDbPathEntryByPathId(rs.getLong("pathid2"));
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
		while (getDb().getUpdateQueueSize() > 0 &&
				(getDb().getUpdateQueueSize(0) > 0 ||
						getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT ||
						getDb().getDontInsertQueueSize() >= DONT_INSERT_QUEUE_SIZE_LIMIT)
				) {
			getDb().consumeOneUpdateQueue();
		}
	}

	private void consumeInsertableQueue() throws SQLException, InterruptedException {
		writelog2("+++ consume insertable queue +++");
		do {
			if (getDb().getUpdateQueueSize() > 0) {
				getDb().consumeOneUpdateQueue();
			} else {
				scheduleLayer3(false,
						new ProxyDirTreeDb.CleanupOrphansCallback() {
					public boolean isEol() throws SQLException, InterruptedException {
						if (getDb().getInsertableQueueSize() > 0) {
							return false;
						} else {
							return true;
						}
					}
				});
			}
		} while (getDb().getInsertableQueueSize() > 0);
		getDb().consumeUpdateQueue(0);
	}

	private void consumeDontInsertQueue() throws SQLException, InterruptedException {
		writelog2("--- consume don't-insert queue ---");
		do {
			if (getDb().getUpdateQueueSize() > 0) {
				getDb().consumeOneUpdateQueue();
			} else {
				scheduleLayer3(false,
						new ProxyDirTreeDb.CleanupOrphansCallback() {
					public boolean isEol() throws SQLException, InterruptedException {
						if (getDb().getDontInsertQueueSize() > 0) {
							return false;
						} else {
							return true;
						}
					}
				});
			}
		} while (getDb().getDontInsertQueueSize() > 0);
		getDb().consumeUpdateQueue(0);
	}

	private static int scheduleLayer2RoundRobinState = 0;
	private static int scheduleLayer2ListDirtiesCounter = 0;
	private void scheduleLayer2(boolean doAllAtOnce) throws SQLException, InterruptedException, IOException {
		consumeSomeUpdateQueue();

		assert(scheduleLayer2RoundRobinState >= 0);
		assert(scheduleLayer2RoundRobinState <= 3);

		if (scheduleLayer2RoundRobinState == 0) {
			writelog2("+++ list (1/4) +++");
			int count = list(null, "(type=1 AND (" + getArchiveExtSubSql() + ")) AND (status=1 OR status=2)", "", "");
			writelog2("+++ list (1/4) finished count=" + count + " +++");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
		}

		if (scheduleLayer2RoundRobinState == 1) {
			consumeInsertableQueue();
			writelog2("+++ list (2/4) +++");
			int count = list(null, "type=0 AND status=2", "", "");
			writelog2("+++ list (2/4) finished count=" + count + " +++");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
		}

		if (scheduleLayer2RoundRobinState == 2) {
			writelog2("+++ list (3/4) +++");
			int count = list(null, "(type=3 AND (" + getArchiveExtSubSql() + ")) AND status=1", "", "");
			writelog2("+++ list (3/4) finished count=" + count + " +++");

			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			}
			scheduleLayer2ListDirtiesCounter = 0;
		}

		if (scheduleLayer2RoundRobinState == 3) {
			consumeInsertableQueue();
			writelog2("+++ list (4/4) +++");
			int count = list(null, "type=0 AND status=1", "", "");
			writelog2("+++ list (4/4) finished count=" + count + " +++");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer2RoundRobinState++;
			} else if (count>0  && scheduleLayer2ListDirtiesCounter < 3) {
				scheduleLayer2RoundRobinState--;
				scheduleLayer2ListDirtiesCounter ++;
			} else if (count==0) {
				setAllCleanFoldersDirty();
			}

		}

		if (!doAllAtOnce) {
			scheduleLayer2RoundRobinState++;
		}
		scheduleLayer2RoundRobinState = scheduleLayer2RoundRobinState % 4;
	}

	private int setAllCleanFoldersDirty() throws SQLException, InterruptedException {
		writelog2("+++ set all clean folders dirty +++");
		String sql = "SELECT * FROM directory WHERE type=0 AND status=0";
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
		writelog2("+++ set all clean folders dirty finished count=" + c2 + " +++");
		return c2;
	}

	private int list(List<Long> dontListRootIds,
			String typeStatusSubSql, String parentTypeSubSql, String orderSubSql)
			throws SQLException, InterruptedException, IOException {
		Statement stmt = getDb().createStatement();

		Dispatcher disp = getDb().getDispatcher();
		disp.setList(Dispatcher.LIST);
		disp.setCsum(Dispatcher.NONE);
		disp.setNoReturn(true);

		String sql = "SELECT d1.*, childhint FROM (SELECT * FROM directory WHERE "
				+ typeStatusSubSql
				+ getDontListRootIdsSubSql(dontListRootIds)
				+ ") AS d1 "
				+ "LEFT JOIN "
				+ "(SELECT DISTINCT parentid AS childhint FROM directory) AS d2 ON pathid=childhint "
				+ "WHERE d1.parentid=0 OR EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid "
				+ parentTypeSubSql
				+ ")"
				+ orderSubSql
				;
		writelog2(sql);
		ResultSet rs = stmt.executeQuery(sql);
		writelog2("+++ list query finished +++");
		int count = 0;
		try {
			while (rs.next()) {
				rs.getLong("childhint");
				if (rs.wasNull()) {
					disp.setNoChildInDb(true);
				} else {
					disp.setNoChildInDb(false);
				}
				DbPathEntry f = getDb().rsToPathEntry(rs);
				Assertion.assertAssertionError(f.isFolder()
						|| ((f.isFile() || f.isCompressedFile()) && ArchiveListerFactory.isArchivable(f)),
						"!! CANNOT LIST THIS ENTRY: " + f.getType() + " at " + f.getPath());
				disp.dispatch(f);
				count++;
				if (getDb().getInsertableQueueSize() >= INSERTABLE_QUEUE_SIZE_LIMIT
						|| getDb().getUpdateQueueSize(0) >= UPDATE_QUEUE_SIZE_LIMIT
						) {
					return count;
				}
			}
		} finally {
			rs.close();
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
	private static int scheduleLayer3RefreshDirectorySizesCounter = 0;
	private void scheduleLayer3(boolean doAllAtOnce, CleanupOrphansCallback callback) throws SQLException, InterruptedException {
		consumeSomeUpdateQueue();

		assert(scheduleLayer3RoundRobinState >= 0);
		assert(scheduleLayer3RoundRobinState <= 5);

		if (scheduleLayer3RoundRobinState == 0) {
			writelog2("*** refresh upperlower entries (1/2) ***");
			int c = getDb().refreshDirectUpperLower(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh upperlower entries (1/2) finished count=" + c + " ***");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			}
		}

		if (scheduleLayer3RoundRobinState == 1) {
			writelog2("*** refresh upperlower entries (2/2) ***");
			int c = getDb().refreshIndirectUpperLower(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh upperlower entries (2/2) finished count=" + c + " ***");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			}
			scheduleLayer3RefreshDirectorySizesCounter = 0;
		}

		if (scheduleLayer3RoundRobinState == 2) {
			writelog2("*** refresh duplicate fields ***");
			int c = getDb().refreshDuplicateFields(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh duplicate fields finished count=" + c + " ***");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			}
		}

		if (scheduleLayer3RoundRobinState == 3) {
			writelog2("*** refresh directory sizes ***");
			int c = getDb().refreshFolderSizes(consumeSomeUpdateQueueRunner);
			writelog2("*** refresh directory sizes finished count=" + c + " ***");
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			} else if (c>0  && scheduleLayer3RefreshDirectorySizesCounter < 10) {
				scheduleLayer3RoundRobinState--;
				scheduleLayer3RefreshDirectorySizesCounter++;
			}
		}

		if (scheduleLayer3RoundRobinState == 4) {
			unlistDisabledExtensions(callback);
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			}
		}

		if (scheduleLayer3RoundRobinState == 5) {
			if (getDb().getUpdateQueueSize(1) == 0) {
				writelog2("*** cleanup orphans ***");
				int c = cleanupOrphans();
				writelog2("*** cleanup orphans finished count=" + c + " ***");
			} else {
				writelog2("*** SKIP cleanup orphans ***");
			}
			if (doAllAtOnce) {
				consumeSomeUpdateQueue();
				scheduleLayer3RoundRobinState++;
			}
		}

		if (!doAllAtOnce) {
			scheduleLayer3RoundRobinState++;
		}
		scheduleLayer3RoundRobinState = scheduleLayer3RoundRobinState % 6;
	}

	private static final int UPDATE_QUEUE_SIZE_LIMIT = 10000;
	private static final int INSERTABLE_QUEUE_SIZE_LIMIT = 100;
	private static final int DONT_INSERT_QUEUE_SIZE_LIMIT = 10000;

	private int cleanupOrphans() throws SQLException, InterruptedException {
		ProxyDirTreeDb.CleanupOrphansCallback cleanupOrphansCallbackRunner =
				new ProxyDirTreeDb.CleanupOrphansCallback() {
			@Override
			public boolean isEol() throws SQLException, InterruptedException {
				if (getDb().getUpdateQueueSize(1) >= UPDATE_QUEUE_SIZE_LIMIT) {
					return true;
				}
				consumeSomeUpdateQueue();
				return false;
			}
		};

		int result = getDb().cleanupOrphansWithChildren(cleanupOrphansCallbackRunner);
		if (result > 0) {
			return result;
		}
		return getDb().cleanupOrphans(cleanupOrphansCallbackRunner);
	}

	private String getArchiveExtSubSql() {
		ArrayList<String> p = new ArrayList<String>();
		HashMap<String, Boolean> eal = getConf().getExtensionAvailabilityMap();
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

	private static String getDontListRootIdsSubSql(List<Long> dontListRootIds) {
		String dontListRootIdsSubSql;
		ArrayList<String> s = new ArrayList<String>();
		if (dontListRootIds != null) {
			for (Long i: dontListRootIds) {
				s.add("rootid<>" + i);
			}
		}
		if (s.size() > 0) {
			dontListRootIdsSubSql = " AND (" + String.join(" AND ", s) + ") ";
		} else {
			dontListRootIdsSubSql = "";
		}
		return dontListRootIdsSubSql;
	}

	private void unlistDisabledExtensions() throws SQLException, InterruptedException {
		unlistDisabledExtensions(null);
	}

	private void unlistDisabledExtensions(ProxyDirTreeDb.CleanupOrphansCallback callback)
			throws SQLException, InterruptedException {
		writelog2("*** unlist disabled extensions ***");
		Statement stmt = getDb().createStatement();
		ArrayList<String> ext = new ArrayList<String>();
		HashMap<String, Boolean> eal = getConf().getExtensionAvailabilityMap();
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
			ResultSet rs = stmt.executeQuery(sql);
			int count = 0;
			try {
				while (rs.next()) {
					DbPathEntry f = getDb().rsToPathEntry(rs);
					Assertion.assertAssertionError(f.isFile() || f.isCompressedFile());
					getDb().updateStatus(f, PathEntry.DIRTY);
					getDb().deleteChildren(f);
					count++;
					if (callback != null) {
						if (callback.isEol()) {
							break;
						}
					}
				}
			} finally {
				rs.close();
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
