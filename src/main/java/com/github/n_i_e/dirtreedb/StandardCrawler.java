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
		cleanupDb_RoundRobinState = 0;
		Statement stmt = getDb().createStatement();
		try {
			while (true) {
				writelog2("--- starting main loop ---");
				getDb().consumeUpdateQueue();

				boolean dontCrawlEquality = false;
				ArrayList<String> s = new ArrayList<String>();
				for (Long i: getDb().getDontInsertRootIdList()) {
					s.add("rootid<>" + i);
					if (i == 0) {
						dontCrawlEquality = true;
					}
				}
				String dontCheckUpdateRootIdsSubSql;
				if (s.size() > 0) {
					dontCheckUpdateRootIdsSubSql = " AND (" + String.join(" AND ", s) + ") ";
				} else {
					dontCheckUpdateRootIdsSubSql = "";
				}

				if (true) {
					writelog2("--- csum (1/2) ---");
					String sql = "SELECT * FROM directory AS d1 WHERE ((type=1 AND status<>2) OR type=3) "
							+ "AND csum IS NULL "
							+ dontCheckUpdateRootIdsSubSql
							+ " AND EXISTS (SELECT * FROM directory AS d2 "
							+ "WHERE (type=1 OR type=3) AND d1.size=d2.size AND d1.pathid<>d2.pathid) "
							//+ "ORDER BY size DESC"
							;
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
							cleanupDbAndListIfRecommended();
							count++;
						}
					} finally {
						rs.close();
					}
					writelog2("--- csum (1/2) finished count=" + count + " ---");
				}

				if (!dontCrawlEquality) {
					crawlEqualityUpdate();
				}

				if (true) {
					writelog2("--- csum (2/2) ---");
					String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR type=3) AND (csum IS NULL OR status=2) "
							+ dontCheckUpdateRootIdsSubSql
							+ " AND EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid AND status=0)";
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
							cleanupDbAndListIfRecommended();
							count++;
						}
					} finally {
						rs.close();
					}
					writelog2("--- csum (2/2) finished count=" + count + " ---");
				}

				if (true) {
					writelog2("--- checkupdate ---");
					Dispatcher disp = getDb().getDispatcher();
					disp.setList(Dispatcher.NONE);
					disp.setCsum(Dispatcher.NONE);
					disp.setNoReturn(true);

					String sql = "SELECT * FROM directory AS d1 WHERE ((type=0 AND status=0) OR type=1) "
							+ dontCheckUpdateRootIdsSubSql
							+ " AND EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid AND type=0 AND status=0)"
							//+ " ORDER BY datelastmodified DESC"
							;
					ResultSet rs = stmt.executeQuery(sql);
					writelog2("--- checkupdate query finished ---");
					int count = 0;
					try {
						while (rs.next()) {
							DbPathEntry f = getDb().rsToPathEntry(rs);
							assert (f.isFolder() || f.isFile());
							disp.dispatch(f);
							cleanupDbAndListIfRecommended();
							count++;
						}
					} finally {
						rs.close();
					}
					writelog2("--- checkupdate finished count=" + count + " ---");
				}

				writelog2("--- complete cleanup/list items ---");
				cleanupDbAndList(true);

				writelog2("--- consume all update queue ---");
				getDb().consumeUpdateQueue();
				writelog2("--- consume all update queue finished ---");
			}
		} catch (SQLException e) {
			writeWarning("Warning", "Caught SQLException, trying to recover (this is usually OK)");
			writelog2("Crawler WARNING caught SQLException, trying to recover");
			e.printStackTrace();
			getDb().discardAllQueueItems();
			getDb().cleanupOrphans();
			getDb().consumeUpdateQueue();
		} finally {
			stmt.close();
		}
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
							getDb().consumeSomeUpdateQueue();
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

	private void crawlEqualityUpdate() throws SQLException, InterruptedException, IOException {
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
				cleanupDbAndListIfRecommended();
				//getDb().consumeSomeUpdateQueue();
				count++;
			}
		} finally {
			rs.close();
		}
		writelog2("--- equality finished count=" + count + " ---");
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

	private void cleanupDbAndListIfRecommended() throws InterruptedException, SQLException, IOException {
		if (getDb().getDontInsertQueueSize() > 100000) {
			cleanupDbAndList(false);
		} else {
			getDb().consumeSomeUpdateQueue();
		}
	}

	private static int cleanupDb_RoundRobinState = 0;
	private static int cleanupDb_RefreshDirectorySizesCounter = 0;
	private void cleanupDbAndList(boolean doAllAtOnce) throws SQLException, InterruptedException, IOException {
		getDb().consumeSomeUpdateQueue();

		boolean isReadyToCleanupDb = getDb().getUpdateQueueSize() == 0;
		if (isReadyToCleanupDb) {
			assert(cleanupDb_RoundRobinState >= 0);
			assert(cleanupDb_RoundRobinState <= 8);

			List<Long> dontListRootIds = getDb().getInsertableRootIdList();
			boolean isReadyToList = getDb().getInsertableQueueSize() < 100000 && getDb().getUpdateQueueSize() == 0;

			if (cleanupDb_RoundRobinState == 0) {
				writelog2("+++ refresh duplicate fields +++");
				getDb().refreshDuplicateFields();
				writelog2("+++ refresh duplicate fields finished +++");
				getDb().consumeSomeUpdateQueue();

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				}
			}

			if (cleanupDb_RoundRobinState == 1) {
				if (isReadyToList) {
					writelog2("+++ refresh upperlower entries (1/2) +++");
					getDb().refreshDirectUpperLower();
					writelog2("+++ refresh upperlower entries (1/2) finished +++");
					getDb().consumeSomeUpdateQueue();
				} else {
					writelog2("+++ SKIP refresh upperlower entries (1/2) +++");
				}

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				}
				cleanupDb_RefreshDirectorySizesCounter = 0;
			}

			if (cleanupDb_RoundRobinState == 2) {
				if (isReadyToList) {
					writelog2("+++ refresh upperlower entries (2/2) +++");
					getDb().refreshIndirectUpperLower();
					writelog2("+++ refresh upperlower entries (2/2) finished +++");
					getDb().consumeSomeUpdateQueue();
				} else {
					writelog2("+++ SKIP refresh upperlower entries (2/2) +++");
				}

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				}
			}

			if (cleanupDb_RoundRobinState == 3) {
				writelog2("+++ refresh directory sizes +++");
				int count = getDb().refreshFolderSizes();
				writelog2("+++ refresh directory sizes finished count=" + count + " +++");
				getDb().consumeSomeUpdateQueue();

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				} else if (count > 0 && cleanupDb_RefreshDirectorySizesCounter < 10) {
					cleanupDb_RoundRobinState--;
					cleanupDb_RefreshDirectorySizesCounter++;
				}
			}

			if (cleanupDb_RoundRobinState == 4) {
				if (isReadyToList) {
					writelog2("+++ list (1/2) +++");
					int count = list(dontListRootIds,
							"((type=1 OR type=3) AND (" + getArchiveExtSubSql() + "))",
							" AND (status=1 OR status=2)",
							"");
					writelog2("+++ list (1/2) finished count=" + count + " +++");
				} else {
					writelog2("+++ SKIP list (1/2) +++");
				}

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				}
			}

			if (cleanupDb_RoundRobinState == 5) {
				if (isReadyToList) {
					writelog2("+++ list (2/2) +++");
					int count = list(dontListRootIds, "type=0", "", "ORDER BY status DESC");
					writelog2("+++ list (2/2) finished count=" + count + " +++");
				} else {
					writelog2("+++ SKIP list (2/2) +++");
				}

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				}
			}

			if (cleanupDb_RoundRobinState == 6) {
				unlistDisabledExtensions();

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				}
			}

			if (cleanupDb_RoundRobinState == 7) {
				if (isReadyToList && !dontListRootIds.contains(0L)) {
					writelog2("+++ equality (new) +++");
					int count = crawlEqualityNew(true, dontListRootIds);
					writelog2("+++ equality (new) finished count=" + count + " +++");
				} else {
					writelog2("+++ SKIP equality (new) +++");
				}

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				}
			}

			if (cleanupDb_RoundRobinState == 8) {
				writelog2("+++ cleanup orphans +++");
				int c = getDb().cleanupOrphans();
				writelog2("+++ cleanup orphans finished count=" + c + " +++");

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				}
			}

/*
			if (cleanupDb_RoundRobinState == 9) {
				writelog2("+++ reset diretories to dirty +++");
				int count = 0;
				Statement stmt = getDb().createStatement();
				try {
					ResultSet rs = stmt.executeQuery("SELECT * FROM directory WHERE type=0 AND status=0");
					writelog2("+++ reset diretories to dirty query finished +++");
					try {
						while (rs.next()) {
							DbPathEntry entry = getDb().rsToPathEntry(rs);
							getDb().updateStatus(entry, PathEntry.DIRTY);
							getDb().consumeSomeUpdateQueue();
							count++;
						}
					} finally {
						rs.close();
					}
				} finally {
					stmt.close();
				}
				writelog2("+++ reset diretories to dirty query finished count=" + count + " +++");

				if (doAllAtOnce) {
					getDb().consumeSomeUpdateQueue();
					cleanupDb_RoundRobinState++;
				}
			}
*/

			if (!doAllAtOnce) {
				cleanupDb_RoundRobinState++;
			}
			cleanupDb_RoundRobinState = cleanupDb_RoundRobinState % 9;
		}
		getDb().consumeSomeUpdateQueue();
	}

	private void list(List<Long> dontListRootIds) throws SQLException, InterruptedException, IOException {
		writelog2("+++ list +++");
		int count = list(dontListRootIds, 
				"(type=0 OR ((type=1 OR type=3) AND (" + getArchiveExtSubSql() + "))) ",
				" AND (status=1 OR status=2 OR parentid=0))",
				"");
		writelog2("+++ list finished count=" + count + " +++");
	}

	private int list(List<Long> dontListRootIds, String typeSubSql, String statusSubSql, String orderSubSql)
			throws SQLException, InterruptedException, IOException {
		Statement stmt = getDb().createStatement();

		Dispatcher disp = getDb().getDispatcher();
		disp.setList(Dispatcher.LIST);
		disp.setCsum(Dispatcher.NONE);
		disp.setNoReturn(true);

		String sql = "SELECT d1.*, childhint FROM (SELECT * FROM directory WHERE "
				+ typeSubSql
				+ getDontListRootIdsSubSql(dontListRootIds)
				+ statusSubSql
				+ ") AS d1 "
				+ "LEFT JOIN "
				+ "(SELECT DISTINCT parentid AS childhint FROM directory) AS d2 ON pathid=childhint "
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
				getDb().consumeSomeUpdateQueue();
				count++;
			}
		} finally {
			rs.close();
		}
		return count;
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
		writelog2("+++ unlist disabled extensions +++");
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
			writelog2("+++ SKIP unlist disabled extensions (nothing to unlist) +++");
		} else {
			dontArchiveExtSubSql = "AND (" + String.join(" OR ", ext) + ")";
			String sql = "SELECT * FROM directory AS d1 WHERE (type=1 OR type=3) " + dontArchiveExtSubSql
					+ " AND EXISTS (SELECT * FROM directory AS d2 WHERE d1.pathid=d2.parentid)";
			ResultSet rs = stmt.executeQuery(sql);
			int count = 0;
			try {
				while (rs.next()) {
					DbPathEntry f = getDb().rsToPathEntry(rs);
					Assertion.assertAssertionError(f.isFile() || f.isCompressedFile());
					getDb().updateStatus(f, PathEntry.DIRTY);
					getDb().deleteChildren(f);
					count++;
				}
			} finally {
				rs.close();
			}
			writelog2("+++ unlist disabled extensions finished count=" + count + " +++");
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
