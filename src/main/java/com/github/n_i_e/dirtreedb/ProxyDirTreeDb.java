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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ProxyDirTreeDb implements IDirTreeDb {
	protected IDirTreeDb parent;

	public ProxyDirTreeDb (IDirTreeDb parent) {
		this.parent = parent;
	}

	@Override
	public void close() throws SQLException {
		parent.close();
	}

	private long _threadHookInterval = 0;
	public void threadHook() throws InterruptedException {
		long n = (new Date()).getTime();
		if (_threadHookInterval != 0 && n - _threadHookInterval > 30*1000) {
			long d = n - _threadHookInterval;
			Debug.writelog("ProxyDirTreeDb threadHookInterval too long: " + d);
		}
		_threadHookInterval = n;
		try {
			((ThreadWithInterruptHook)Thread.currentThread()).interruptHook();
		} catch (ClassCastException e) {
			Thread.sleep(0);
		}
	}

	@Override
	public Statement createStatement() throws SQLException, InterruptedException {
		threadHook();
		return parent.createStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException, InterruptedException {
		threadHook();
		return parent.prepareStatement(sql);
	}

	@Override
	public DbPathEntry rsToPathEntry(ResultSet rs, String prefix) throws SQLException, InterruptedException {
		threadHook();
		return parent.rsToPathEntry(rs, prefix);
	}

	public DbPathEntry rsToPathEntry(ResultSet rs) throws SQLException, InterruptedException {
		return rsToPathEntry(rs, "");
	}

	@Override
	public void insert(DbPathEntry basedir, PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(newentry != null);
		threadHook();
		try {
			parent.insert(basedir, newentry);
		} catch (SQLException e) {
			if (reviveOprhan(basedir, newentry) == 0) {
				parent.insert(basedir, newentry);
			}
		}
	}

	@Override
	public void update(DbPathEntry oldentry, PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(oldentry != null);
		Assertion.assertNullPointerException(newentry != null);
		Assertion.assertAssertionError(oldentry.getPath().equals(newentry.getPath()),
				"!! old and new entry paths do not match:\nold=" + oldentry.getPath() + "\nnew=" + newentry.getPath());
		Assertion.assertAssertionError(oldentry.getType() == newentry.getType());

		if ((!PathEntry.dscMatch(oldentry, newentry)) || (!PathEntry.csumMatch(oldentry, newentry))) {
			deleteEquality(oldentry.getPathId());
			threadHook();
			parent.update(oldentry, newentry);
		} else if (oldentry.getStatus() != newentry.getStatus()) {
			threadHook();
			parent.updateStatus(oldentry, newentry.getStatus());
		}
	}

	@Override
	public void updateStatus(DbPathEntry entry, int newstatus) throws SQLException, InterruptedException {
		assert(entry != null);
		threadHook();
		parent.updateStatus(entry, newstatus);
	}

	public void updateStatuses(Iterator<DbPathEntry> entries, int newstatus)
			throws SQLException, InterruptedException {
		while (entries.hasNext()) {
			DbPathEntry entry = entries.next();
			updateStatus(entry, newstatus);
		}
	}

	@Override
	public void delete(final DbPathEntry entry) throws SQLException, InterruptedException {
		threadHook();
		parent.delete(entry);
	}

	protected void deleteLowPriority(final DbPathEntry entry) throws SQLException, InterruptedException {
		delete(entry);
	}

	public void deleteChildren(final DbPathEntry entry) throws SQLException, InterruptedException {
		threadHook();
		PreparedStatement ps = prepareStatement("SELECT * FROM directory WHERE parentid=?");
		ps.setLong(1, entry.getPathId());
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				delete(rsToPathEntry(rs));
			}
		} finally {
			rs.close();
			ps.close();
		}
	}

	public void deleteEquality(long pathid) throws InterruptedException, SQLException {
		threadHook();
		String sql = "SELECT * FROM equality WHERE pathid1=? OR pathid2=?";
		PreparedStatement ps = prepareStatement(sql);
		ps.setLong(1, pathid);
		ps.setLong(2, pathid);
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				threadHook();
				parent.deleteEquality(rs.getLong("pathid1"), rs.getLong("pathid2"));
			}
		} finally {
			rs.close();
			ps.close();
		}
	}

	public void deleteUpperLower(long pathid) throws SQLException, InterruptedException {
		threadHook();
		PreparedStatement ps;
		String sql = "SELECT * FROM upperlower WHERE upper=? OR lower=?";
		ps = prepareStatement(sql);
		ps.setLong(1, pathid);
		ps.setLong(2, pathid);
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				threadHook();
				parent.deleteUpperLower(rs.getLong("upper"), rs.getLong("lower"));
			}
		} finally {
			rs.close();
			ps.close();
		}
	}

	@Override
	public void unsetClean(long pathid) throws SQLException, InterruptedException {
		threadHook();
		parent.unsetClean(pathid);
	}

	@Override
	public void disable(DbPathEntry entry) throws SQLException, InterruptedException {
		threadHook();
		parent.disable(entry);
	}

	@Override
	public void disable(DbPathEntry entry, PathEntry newentry) throws SQLException, InterruptedException {
		threadHook();
		parent.disable(entry, newentry);
	}

	@Override
	public void updateParentId(DbPathEntry entry, long newparentid) throws SQLException ,InterruptedException {
		threadHook();
		parent.updateParentId(entry, newparentid);
	};

	@Override
	public void orphanize(DbPathEntry entry) throws SQLException, InterruptedException {
		threadHook();
		parent.orphanize(entry);
	}

	public void orphanizeChildren(final DbPathEntry entry) throws SQLException, InterruptedException {
		threadHook();
		PreparedStatement ps = prepareStatement("SELECT * FROM directory WHERE parentid=?");
		ps.setLong(1, entry.getPathId());
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				orphanize(rsToPathEntry(rs));
			}
		} finally {
			rs.close();
			ps.close();
		}
	}

	@Override
	public void insertUpperLower(long upper, long lower, int distance) throws SQLException, InterruptedException {
		threadHook();
		parent.insertUpperLower(upper, lower, distance);
	}

	@Override
	public void deleteUpperLower(long upper, long lower) throws SQLException, InterruptedException {
		threadHook();
		parent.deleteUpperLower(upper, lower);
	}

	@Override
	public void insertEquality(long pathid1, long pathid2, long size, int csum)
			throws SQLException, InterruptedException {
		threadHook();
		parent.insertEquality(pathid1, pathid2, size, csum);
	}

	@Override
	public void deleteEquality(long pathid1, long pathid2) throws InterruptedException, SQLException {
		threadHook();
		parent.deleteEquality(pathid1, pathid2);
	}

	@Override
	public void updateEquality(long pathid1, long pathid2) throws InterruptedException, SQLException {
		threadHook();
		parent.updateEquality(pathid1, pathid2);
	}

	public void insertOrUpdateEquality(long pathid1, long pathid2, long size, int csum)
			throws InterruptedException, SQLException {
		threadHook();
		PreparedStatement ps = prepareStatement("SELECT * FROM equality WHERE pathid1=? AND pathid2=?");
		try {
			if (pathid1>pathid2) {
				ps.setLong(1, pathid1);
				ps.setLong(2, pathid2);
			} else {
				ps.setLong(1, pathid2);
				ps.setLong(2, pathid1);
			}
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					updateEquality(pathid1, pathid2);
				} else {
					insertEquality(pathid1, pathid2, size, csum);
				}
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}

	}

	@Override
	public void updateDuplicateFields(long pathid, long duplicate, long dedupablesize)
			throws InterruptedException, SQLException {
		threadHook();
		parent.updateDuplicateFields(pathid, duplicate, dedupablesize);
	}

	public DbPathEntry getParent(DbPathEntry basedir) throws SQLException, InterruptedException {
		threadHook();
		PreparedStatement ps = prepareStatement("select * from DIRECTORY where PATHID=?");
		ps.setLong(1, basedir.getParentId());
		ResultSet rs = ps.executeQuery();
		try {
			if (!rs.next()) {
				// root or oprhan directory
				return null;
			}
			return rsToPathEntry(rs);
		} finally {
			rs.close();
			ps.close();
		}
	}

	public List<DbPathEntry> getCompressionStack(DbPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertAssertionError(entry.isFile() || entry.isCompressedFolder() || entry.isCompressedFile());
		threadHook();

		ArrayList<DbPathEntry> result = new ArrayList<DbPathEntry>();
		result.add(entry);

		DbPathEntry cursor = entry;
		while(!cursor.isFile()) {
			Assertion.assertAssertionError(cursor.isCompressedFolder() || cursor.isCompressedFile());
			threadHook();
			cursor = getParent(cursor);
			if (cursor == null) {
				return null; // orphan
			}
			Assertion.assertAssertionError(cursor.isFile() || cursor.isCompressedFile());
			result.add(cursor);
		}
		assert(result.size()>0);
		return result;
	}

	public Map<String, DbPathEntry> childrenList(DbPathEntry entry) throws SQLException, InterruptedException {
		threadHook();

		Map<String, DbPathEntry> result = new HashMap<String, DbPathEntry>();

		PreparedStatement ps = prepareStatement("SELECT * FROM directory WHERE parentid=?");
		ps.setLong(1, entry.getPathId());
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				threadHook();
				DbPathEntry f = rsToPathEntry(rs);
				result.put(f.getPath(), f);
			}
		} finally {
			rs.close();
			ps.close();
		}

		return result;
	}

	public InputStream getInputStream(DbPathEntry entry) throws SQLException, IOException, InterruptedException {
		Assertion.assertAssertionError(entry.isFile() || entry.isCompressedFile()); // File / CompressedFile
		return getInputStream(getCompressionStack(entry));
	}

	public InputStream getInputStream(List<DbPathEntry> stack) throws SQLException, IOException, InterruptedException {
		if (stack == null) { return null; } // orphan
		Assertion.assertAssertionError(stack.size()>0);
		DbPathEntry entry = stack.get(stack.size()-1);
		try {
			if (stack.size() == 1) {
				return entry.getInputStream();
			} else {
				Assertion.assertAssertionError(entry.isFile());
				InputStream result = null;
				for (int i=stack.size()-2; i>=0; i--) {
					DbPathEntry parent = entry;
					InputStream parentStream = result;
					entry = stack.get(i);
					Assertion.assertAssertionError(entry.isCompressedFile(),
							"wrong element " + stack.get(i).getPath() + ", type=" + entry.getType());
					PathEntryLister z;
					if (parent.isFile()) {
						z = PathEntryListerFactory.getInstance(parent);
					} else {
						z = PathEntryListerFactory.getInstance(parent, parentStream);
					}
					result = z.getInputStream(entry);
					Assertion.assertAssertionError(result != null);
				}
				return result;
			}
		} catch (IOException e) {
			disable(entry);
			throw e;
		}
	}

	public DbPathEntry getDbPathEntryByPathId(long pathid) throws SQLException, InterruptedException {
		String sql = "SELECT * from DIRECTORY where PATHID=?";
		PreparedStatement ps = prepareStatement(sql);
		ps.setLong(1, pathid);
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				DbPathEntry p = rsToPathEntry(rs);
				if (p.getPathId() == pathid) {
					return p;
				}
			}
		} finally {
			rs.close();
			ps.close();
		}
		return null;
	}

	public DbPathEntry getDbPathEntryByPath(String path) throws SQLException, InterruptedException {
		String sql = "SELECT * from DIRECTORY where PATH=?";
		PreparedStatement ps = prepareStatement(sql);
		ps.setString(1, path);
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				DbPathEntry p = rsToPathEntry(rs);
				if (p.getPath().equals(path)) {
					return p;
				}
			}
		} finally {
			rs.close();
			ps.close();
		}
		return null;
	}

	public int cleanupEqualityOrphans(IsEol isEol) throws SQLException, InterruptedException {
		String sql = "SELECT * FROM equality "
				+ "WHERE NOT EXISTS (SELECT * FROM directory WHERE pathid1=pathid) "
				+ "OR NOT EXISTS (SELECT * FROM directory WHERE pathid2=pathid)";
		PreparedStatement ps = prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		try {
			int count = 0;
			while (rs.next()) {
				deleteUpperLower(rs.getLong("pathid1"), rs.getLong("pathid2"));
				count ++;
				if (isEol.isEol()) { break; }
			}
			return count;
		} finally {
			rs.close();
			ps.close();
		}
	}

	public int cleanupUpperLowerOrphans(IsEol isEol)
			throws SQLException, InterruptedException {
		String sql = "SELECT * FROM upperlower "
				+ "WHERE NOT EXISTS (SELECT * FROM directory WHERE upper=pathid) "
				+ "OR NOT EXISTS (SELECT * FROM directory WHERE lower=pathid)";
		PreparedStatement ps = prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		try {
			int count = 0;
			while (rs.next()) {
				deleteUpperLower(rs.getLong("upper"), rs.getLong("lower"));
				count ++;
				if (isEol.isEol()) { break; }
			}
			return count;
		} finally {
			rs.close();
			ps.close();
		}
	}

	/*
	 * An orphan entry is a DIRECTORY entry with invalid PARENTID (there is no row with PATHID of that number).
	 */
	private int cleanupOrphans(PreparedStatement ps, IsEol isEol)
					throws SQLException, InterruptedException {
		try {
			ResultSet rs = ps.executeQuery();
			try {
				int count = 0;
				while (rs.next()) {
					threadHook();
					delete(rsToPathEntry(rs));
					count++;
					if (isEol != null) {
						if (isEol.isEol()) {
							return count;
						}
					}
				}
				return count;
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
	}

	@Deprecated
	private int cleanupOrphans(String path, IsEol isEol)
					throws SQLException, InterruptedException {
		PreparedStatement ps;
		Assertion.assertNullPointerException(path != null);
		String sql = "SELECT * FROM directory AS d1 WHERE parentid<>0 AND path=? "
				+ "AND NOT EXISTS (SELECT * FROM directory AS d2 WHERE d1.parentid=d2.pathid)";
		ps = prepareStatement(sql);
		ps.setString(1, path);
		return cleanupOrphans(ps, isEol);
	}

	@Deprecated
	public int cleanupOrphans(String path) throws SQLException, InterruptedException {
		return cleanupOrphans(path, null);
	}

	@Deprecated
	public int cleanupOrphansWithoutChildren(IsEol isEol)
					throws SQLException, InterruptedException {
		PreparedStatement ps;
		String sql = "SELECT * FROM directory AS d1 WHERE parentid<>0 "
				+ "AND NOT EXISTS (SELECT * FROM directory AS d2 WHERE d1.parentid=d2.pathid) "
				+ "AND NOT EXISTS (SELECT * FROM directory AS d3 WHERE d1.pathid=d3.parentid)";
		ps = prepareStatement(sql);
		return cleanupOrphans(ps, isEol);
	}

	public int cleanupOrphans(IsEol isEol)
					throws SQLException, InterruptedException {
		PreparedStatement ps;
		String sql = "SELECT * FROM directory AS d1 WHERE parentid<>0 "
				+ "AND NOT EXISTS (SELECT * FROM directory AS d2 WHERE d1.parentid=d2.pathid)";
		ps = prepareStatement(sql);
		return cleanupOrphans(ps, isEol);
	}

	public int cleanupOrphansWithChildren(IsEol isEol)
			throws SQLException, InterruptedException {
		PreparedStatement ps;
		String sql = "SELECT * FROM directory AS d1 WHERE parentid<>0 "
				+ "AND NOT EXISTS (SELECT * FROM directory AS d2 WHERE d1.parentid=d2.pathid) "
				+ "AND EXISTS (SELECT * FROM directory AS d3 WHERE d1.pathid=d3.parentid)";
		ps = prepareStatement(sql);
		return cleanupOrphans(ps, isEol);
	}

	public int cleanupOrphans() throws SQLException, InterruptedException {
		return cleanupOrphans((IsEol)null);
	}

	public void cleanupOrphansAll() throws SQLException, InterruptedException {
		while (cleanupOrphans() > 0) {}
	}

	private int reviveOprhan(final DbPathEntry basedir, final PathEntry newentry)
			throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(basedir != null);
		Assertion.assertNullPointerException(newentry != null);
		Assertion.assertAssertionError(!newentry.isClean());
		String sql = "SELECT * FROM directory AS d1 WHERE path=?"
				+ " AND NOT EXISTS (SELECT * FROM directory AS d2 WHERE d1.parentid=d2.pathid)";
		PreparedStatement ps = prepareStatement(sql);
		ps.setString(1, newentry.getPath());
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				DbPathEntry oldentry = rsToPathEntry(rs);
				if (newentry.getPath().equals(oldentry.getPath())) {
					Assertion.assertSQLException(oldentry.getType() == newentry.getType());
					update(oldentry, newentry);
					updateParentId(oldentry, basedir.getPathId());
					return 1;
				}
			}
			return 0;
		} finally {
			rs.close();
			ps.close();
		}
	}

	public int refreshDirectUpperLower() throws SQLException, InterruptedException {
		return refreshDirectUpperLower((IsEol)null);
	}

	public int refreshDirectUpperLower(IsEol isEol)
			throws SQLException, InterruptedException {
		return refreshDirectUpperLower(null, isEol);
	}

	public int refreshDirectUpperLower(Set<Long> dontListRootIds) throws SQLException, InterruptedException {
		return refreshDirectUpperLower(dontListRootIds, null);
	}

	public int refreshDirectUpperLower(Set<Long> dontListRootIds, IsEol isEol)
			throws SQLException, InterruptedException {
		threadHook();
		Statement stmt = createStatement();
		int count = 0;
		try {
			threadHook();
			ResultSet rs = stmt.executeQuery("SELECT parentid, pathid FROM directory AS d1 WHERE parentid<>0 "
					+ getDontListRootIdsSubSql(dontListRootIds)
					+ "AND EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid) " // NOT orphan
					+ "AND NOT EXISTS (SELECT * FROM upperlower WHERE distance=1 AND parentid=upper AND pathid=lower)");
			try {
				while (rs.next()) {
					threadHook();
					insertUpperLower(rs.getLong("parentid"), rs.getLong("pathid"), 1);
					count++;
					if (isEol != null) {
						if (isEol.isEol()) {
							return count;
						}
					}
				}
			} finally {
				rs.close();
			}
		} finally {
			stmt.close();
		}
		return count;
	}

	public int refreshIndirectUpperLower() throws SQLException, InterruptedException {
		return refreshIndirectUpperLower((IsEol)null);
	}

	public int refreshIndirectUpperLower(IsEol isEol)
					throws SQLException, InterruptedException {
		return refreshIndirectUpperLower(null, isEol);
	}

	public int refreshIndirectUpperLower(Set<Long> dontListRootIds) throws SQLException, InterruptedException {
		return refreshIndirectUpperLower(dontListRootIds, null);
	}

	public int refreshIndirectUpperLower(Set<Long> dontListRootIds, IsEol isEol)
					throws SQLException, InterruptedException {
		Statement stmt = createStatement();
		int count = 0;
		try {
			threadHook();
			ResultSet rs = stmt.executeQuery("SELECT u1.upper, pathid AS lower, u1.distance+1 AS distance "
					+ "FROM upperlower AS u1, directory "
					+ "WHERE u1.lower=parentid "
					+ getDontListRootIdsSubSql(dontListRootIds)
					+ "AND NOT EXISTS (SELECT * FROM upperlower WHERE upper=u1.upper AND lower=pathid)");
			try {
				HashMap<Long, ArrayList<Long>> d = new HashMap<Long, ArrayList<Long>>();
				while (rs.next()) {
					Long u = rs.getLong("upper");
					Long l = rs.getLong("lower");
					if (d.get(u) == null) {
						d.put(u, new ArrayList<Long>());
					}
					if (!d.get(u).contains(l)) {
						d.get(u).add(l);
						threadHook();
						insertUpperLower(u, l, rs.getInt("distance"));
					}
					count++;
					if (isEol != null) {
						if (isEol.isEol()) {
							return count;
						}
					}
				}
			} finally {
				rs.close();
			}
		} finally {
			stmt.close();
		}
		return count;
	}

	private static String getDontListRootIdsSubSql(Set<Long> dontListRootIds) {
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

	public void refreshFolderSizesAll() throws SQLException, InterruptedException {
		threadHook();
		while (refreshFolderSizes() > 0) {}
	}
	public int refreshFolderSizes() throws SQLException, InterruptedException {
		return refreshFolderSizes(null);
	}

	public int refreshFolderSizes(IsEol isEol)
			throws SQLException, InterruptedException {
		Statement stmt = createStatement();
		ResultSet rs = stmt.executeQuery("SELECT d1.*, newsize, newcompressedsize FROM "
				+ "(SELECT * FROM directory WHERE type=0) as d1, "
				+ "(SELECT parentid, SUM(size) AS newsize, SUM(compressedsize) AS newcompressedsize FROM directory "
				+ "WHERE size>=0 GROUP BY parentid) AS d2 "
				+ "WHERE d1.pathid=d2.parentid AND (d1.size<>d2.newsize OR d1.compressedsize<>d2.newcompressedsize)");
		try {
			int count=0;
			while (rs.next()) {
				threadHook();
				DbPathEntry entry = rsToPathEntry(rs);
				PathEntry newentry = new PathEntry(entry);
				newentry.setSize(rs.getLong("newsize"));
				newentry.setCompressedSize(rs.getLong("newcompressedsize"));
				update(entry, newentry);
				count++;
				if (isEol != null) {
					if (isEol.isEol()) {
						return count;
					}
				}
			}
			return count;
		} finally {
			rs.close();
			stmt.close();
		}
	}

	public int refreshDuplicateFields() throws InterruptedException, SQLException {
		return refreshDuplicateFields(null);
	}

	public int refreshDuplicateFields(IsEol isEol)
			throws InterruptedException, SQLException {
		threadHook();

		Statement stmt1 = createStatement();
		int count = 0;
		try {
			ResultSet rs = stmt1.executeQuery("SELECT pathid, newduplicate, newdedupablesize FROM directory AS d1,"
					+ " (SELECT size, csum, count(size)-1 AS newduplicate, (count(size)-1)*size AS newdedupablesize"
					+ " FROM directory AS d2 WHERE (type=1 OR type=3) AND csum IS NOT NULL"
					+ " AND EXISTS (SELECT * FROM directory AS d3 WHERE d2.parentid=d3.pathid)"
					+ " GROUP BY size, csum"
					+ " HAVING (COUNT(size)>=2 AND COUNT(size)-1<>MAX(duplicate))"
					+ " OR (COUNT(size)>=2 AND COUNT(size)-1<>MAX(duplicate))"
					+ " OR MAX(duplicate)>MIN(duplicate)) AS d4"
					+ " WHERE (d1.type=1 OR d1.type=3) AND d1.size=d4.size AND d1.csum=d4.csum"
					+ " AND EXISTS (SELECT * FROM directory AS d5 WHERE d1.parentid=d5.pathid)");
			try {
				while (rs.next()) {
					threadHook();
					updateDuplicateFields(rs.getLong("pathid"), rs.getLong("newduplicate"), rs.getLong("newdedupablesize"));
					count++;
					if (isEol != null) {
						if (isEol.isEol()) {
							return count;
						}
					}
				}
			} finally {
				rs.close();
			}
		} finally {
			stmt1.close();
		}

		Statement stmt2 = createStatement();
		try {
			ResultSet rs = stmt2.executeQuery("SELECT * FROM directory WHERE (duplicate>0 OR dedupablesize>0) "
					+ "AND (type=0 OR type=2 OR csum IS NULL)");
			try {
				while (rs.next()) {
					threadHook();
					updateDuplicateFields(rs.getLong("pathid"), 0, 0);
					count++;
					if (isEol != null) {
						if (isEol.isEol()) {
							return count;
						}
					}
				}
			} finally {
				rs.close();
			}
		} finally {
			stmt2.close();
		}
		return count;
	}

	public static File getFileIfExists(final PathEntry entry) {
		Assertion.assertAssertionError(entry.getType() == PathEntry.FOLDER || entry.getType() == PathEntry.FILE,
				"Assertion Error: expected type for path <" + entry.getPath() + "> 0 (folder) or 1 (file), was " + entry.getType());

		File result = new File(entry.getPath());
		if (!result.exists()
				|| (entry.getType() == PathEntry.FOLDER && !result.isDirectory())
				|| (entry.getType() == PathEntry.FILE && !result.isFile())
				) {
			// basedir does not exist
			return null;
		}
		String s = result.getPath();
		if (entry.getType() == PathEntry.FOLDER && !s.endsWith("\\")) {
			s += "\\";
		}
		if (!s.equals(entry.getPath())) {
			Assertion.assertAssertionError(s.equalsIgnoreCase(entry.getPath()));
			return null;
		}
		return result;
	}

	public static PathEntry getNewPathEntry(final PathEntry entry) throws SQLException, IOException {
		return getNewPathEntry(entry, getFileIfExists(entry));
	}

	public static PathEntry getNewPathEntry(final PathEntry entry, final File fileobj) throws SQLException, IOException {
		if (entry.isFolder() || entry.isFile()) {
			if (fileobj == null) {
				throw new FileNotFoundException("!! File Not Found: " + entry.getPath());
			}
			PathEntry result = new PathEntry(fileobj);
			if (!entry.getPath().equals(result.getPath())) {
				throw new FileNotFoundException("!! Old and new paths do not match (possibly upper and lower case): "
						+ entry.getPath() + " != " + result.getPath());
			}
			if (entry.isFolder()) {
				result.setSize(entry.getSize());
				result.setCompressedSize(entry.getCompressedSize());
				if (PathEntry.dMatch(entry, result)) {
					result.setStatus(entry.getStatus());
				}
			} else { // isFile
				if (entry.getSize() < 0) { // size is sometimes <0; JavaVM bug?
					entry.setCsumAndClose(entry.getInputStream());
				}
				if (PathEntry.dscMatch(entry, result)) {
					if (!entry.isCsumNull()) {
						result.setCsum(entry.getCsum());
					}
					result.setStatus(entry.getStatus());
				}
			}
			return result;
		} else {
			return new PathEntry(entry);
		}
	}

	public Dispatcher getDispatcher() {
		Debug.writelog("This is SingleThread Dispatcher");
		return new Dispatcher();
	}

	public class Dispatcher {
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

		public PathEntry dispatch(final DbPathEntry entry) throws IOException, InterruptedException, SQLException {
			threadHook();
			if (entry == null || !isReachableRoot(entry.getRootId())) {
				return null;
			}
			if (entry.isFolder()) {
				return dispatchFolder(entry);
			} else if (entry.isFile()) {
				return dispatchFile(entry);
			} else if (entry.isCompressedFile()) {
				return dispatchCompressedFile(entry);
			} else { // COMPRESSEDFOLDER - DON'T DO ANYTHING
				return new PathEntry(entry);
			}
		}

		protected PathEntry dispatchFolder(final DbPathEntry entry)
				throws SQLException, InterruptedException {
			threadHook();

			File fileobj = getFileIfExists(entry);
			if (fileobj == null) {
				checkRootAndDisable(entry);
				return null;
			}

			final PathEntry newentry;
			try {
				newentry = getNewPathEntry(entry, fileobj);
			} catch (IOException e) {
				checkRootAndDisable(entry);
				return null;
			}

			newentry.setSize(entry.getSize());
			newentry.setCompressedSize(entry.getCompressedSize());

			if (entry.isClean() && PathEntry.dMatch(entry, newentry)) {
				return newentry; // no change
			}

			try {
				final DirLister newfolderIter;
				if (isList()) {
					newfolderIter = new DirLister(entry, fileobj);
				} else {
					newfolderIter = null;
				}

				final Map<String, DbPathEntry> oldfolder;
				if (!isList() || newfolderIter == null) {
					oldfolder = null;
				} else if (isNoChildInDb()) {
					oldfolder = new HashMap<String, DbPathEntry>();
				} else {
					oldfolder = childrenList(entry);
				}

				if (!isList()) {
					if (!entry.isDirty() && !PathEntry.dMatch(entry, newentry)) {
						updateStatus(entry, PathEntry.DIRTY);
					}
				} else if (isList() && newfolderIter != null && oldfolder != null) {
					dispatchFolderListCore(entry, fileobj, oldfolder, newentry, newfolderIter);
				}
			} catch (IOException e) {
				checkRootAndDisable(entry);
			}
			return newentry;
		}

		protected PathEntry dispatchFile(final DbPathEntry entry)
				throws SQLException, InterruptedException {
			Assertion.assertAssertionError(entry.isFile());
			threadHook();

			final PathEntry newentry;
			try {
				newentry = getNewPathEntry(entry);
			} catch (IOException e) {
				checkRootAndDisable(entry);
				return null;
			}

			if (PathEntry.dscMatch(entry, newentry)) {
				if (!entry.isCsumNull()) {
					newentry.setCsum(entry.getCsum());
				}
				newentry.setStatus(entry.getStatus());
			}

			try {
				final PathEntryLister newfolderIter;
				if (isList() && (!entry.isClean() || !PathEntry.dscMatch(entry, newentry))) {
					newfolderIter = PathEntryListerFactory.getInstance(entry);
					newfolderIter.setCsumRequested(PathEntryListerFactory.isCsumRecommended(entry));

				} else {
					newfolderIter = null;
				}

				final Map<String, DbPathEntry> oldfolder;
				if (!isList() || newfolderIter == null) {
					oldfolder = null;
				} else if (isNoChildInDb()) {
					oldfolder = new HashMap<String, DbPathEntry>();
				} else {
					oldfolder = childrenList(entry);
				}

				if (oldfolder == null) { // not isList()
					if (!entry.isDirty() && !PathEntry.dscMatch(entry, newentry)) {
						newentry.setStatus(PathEntry.DIRTY);
					}
				} else {
					assert(newfolderIter != null);
					dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
				}
				if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !PathEntry.dscMatch(entry, newentry)))) {
					newentry.setCsumAndClose(newentry.getInputStream());
					if (newentry.isNoAccess()) {
						newentry.setStatus(PathEntry.DIRTY);
					}
				}
				update(entry, newentry);
			} catch (IOException e) {
				checkRootAndDisable(entry);
			}
			return newentry;
		}

		protected PathEntry dispatchCompressedFile(final DbPathEntry entry) throws SQLException, InterruptedException {
			threadHook();

			final PathEntry newentry = new PathEntry(entry);
			final List<DbPathEntry> stack = getCompressionStack(entry);
			if (stack == null) { return newentry; } // orphan
			try {
				if (isList()) {
					final PathEntryLister newfolderIter;
					if (!entry.isClean()) {
						newfolderIter = PathEntryListerFactory.getInstance(entry, getInputStream(stack));
						newfolderIter.setCsumRequested(PathEntryListerFactory.isCsumRecommended(entry));
					} else {
						newfolderIter = null;
					}

					final Map<String, DbPathEntry> oldfolder;
					if (newfolderIter == null) {
						oldfolder = null;
					} else if (isNoChildInDb()) {
						oldfolder = new HashMap<String, DbPathEntry>();
					} else {
						oldfolder = childrenList(entry);
					}

					if (oldfolder != null) { // isList()
						assert(newfolderIter != null);
						dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
					}
				}

				if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !PathEntry.dscMatch(entry, newentry)))) {
					assert(stack != null);
					newentry.setCsumAndClose(getInputStream(stack));
					if (newentry.isNoAccess()) {
						newentry.setStatus(PathEntry.DIRTY);
					}
				}
				update(entry, newentry);
			} catch (IOException e) {
				checkRootAndDisable(entry);
			}
			return newentry;
		}

		protected void dispatchFolderListCore(
				DbPathEntry entry,
				File fileobj,
				Map<String, DbPathEntry> oldfolder,
				PathEntry newentry,
				DirLister newfolderIter
				) throws InterruptedException, SQLException, IOException {
			long new_size = 0;
			long new_compressedsize = 0;

			final List<DbPathEntry> updatedfolders = new ArrayList<DbPathEntry>();
			long t0 = new Date().getTime();
			long count=0;
			while (newfolderIter.hasNext()) {
				Thread.sleep(0);
				PathEntry newchild = newfolderIter.next();
				Assertion.assertAssertionError(newchild.isFolder() || newchild.isFile());

				count++;
				long t1 = new Date().getTime();
				if (t1-t0 > 2*60*1000) {
					Debug.writelog("dispatchFolderListCore loop too long, still ongoing: basedir=<" + entry.getPath() + ">, "
							+ "current child=<" + newchild.getPath() + ">, count=" + count + ", "
							+ "isListCsum=" + isListCsum() + ", oldfoldersize=" + oldfolder.size());
					t0 = t1;
				}

				DbPathEntry oldchild = oldfolder.get(newchild.getPath());
				if (oldchild != null) { // exists in oldfolder - update
					if (newchild.isFolder()) {
						if (oldchild.isClean() && !PathEntry.dMatch(oldchild, newchild)) {
							updatedfolders.add(oldchild);
						}
						if (oldchild.getSize() >= 0) {
							new_size += oldchild.getSize();
							new_compressedsize += oldchild.getCompressedSize();
						}
					} else { // FILE
						assert(newchild.isFile());
						if (isListCsumForce() || (isListCsum() && (oldchild.isCsumNull() || !PathEntry.dscMatch(oldchild, newchild)))) {
							try {
								newchild.setCsumAndClose(newchild.getInputStream());
							} catch (IOException e) {
								newchild.setStatus(PathEntry.NOACCESS);
							}
							update(oldchild, newchild);
						} else if (!PathEntry.dscMatch(oldchild, newchild)) {
							update(oldchild, newchild);
						} else if (oldchild.isNoAccess() != newchild.isNoAccess()) {
							updateStatus(oldchild, newchild.getStatus());
						}
						if (newchild.getSize() >= 0) {
							new_size += newchild.getSize();
							new_compressedsize += newchild.getCompressedSize();
						}
					}
					oldfolder.remove(newchild.getPath());
				} else { // not in oldfolder - insert
					if (isListCsum() && newchild.isFile()) {
						try {
							newchild.setCsumAndClose(newchild.getInputStream());
						} catch (IOException e) {
							newchild.setStatus(PathEntry.NOACCESS);
						}
					}
					insert(entry, newchild);
					if (newchild.isFile()) {
						if (newchild.getSize() >= 0) {
							new_size += newchild.getSize();
							new_compressedsize += newchild.getCompressedSize();
						}
					}
				}
			}

			updateStatuses(updatedfolders.iterator(), PathEntry.DIRTY);
			for (DbPathEntry p: oldfolder.values()) {
				Assertion.assertFileNotFoundException(getFileIfExists(p) == null,
						"!! File DOES exist: " + p.getPath()
						);
				orphanize(p);
			}

			newentry.setSize(new_size);
			newentry.setCompressedSize(new_compressedsize);
			newentry.setStatus(PathEntry.CLEAN);
			update(entry, newentry);
		}

		protected void dispatchFileListCore(
				DbPathEntry entry,
				Map<String, DbPathEntry> oldfolder,
				PathEntry newentry,
				PathEntryLister newfolderIter
				) throws InterruptedException, SQLException, IOException {
			long t0 = new Date().getTime();
			long count=0;
			while (newfolderIter.hasNext()) {
				Thread.sleep(0);
				PathEntry newchild = newfolderIter.next();
				Assertion.assertNullPointerException(newchild != null, "newchild is null, entry=" + entry.getPath());
				Assertion.assertAssertionError(newchild.isCompressedFolder() || newchild.isCompressedFile(),
						"newchild type error: " + newchild.getType() + " is not CompressedFolder nor CompressedFile, entry=" + entry.getPath());

				count++;
				long t1 = new Date().getTime();
				if (t1-t0 > 2*60*1000) {
					Debug.writelog("dispatchFileListCore loop too long, still ongoing: basedir=<" + entry.getPath() + ">, "
							+ "current child=<" + newchild.getPath() + ">, count=" + count + ", "
							+ "isListCsum=" + isListCsum() + ", oldfoldersize=" + oldfolder.size());
					t0 = t1;
				}
				DbPathEntry oldchild = oldfolder.get(newchild.getPath());
				if (oldchild != null) {
					if (!PathEntry.dscMatch(oldchild, newchild)) {
						update(oldchild, newchild);
					} else if (oldchild.isNoAccess() != newchild.isNoAccess()) {
						updateStatus(oldchild, newchild.getStatus());
					}
					oldfolder.remove(newchild.getPath());
				} else {
					insert(entry, newchild);
				}
			}
			newfolderIter.close();
			for (DbPathEntry p: oldfolder.values()) {
				Assertion.assertAssertionError(p.getParentId()!=0);
				orphanize(p);
			}
			newentry.setStatus(PathEntry.CLEAN);
			update(entry, newentry);
		}

		public boolean checkEquality(final DbPathEntry entry1, final DbPathEntry entry2, final int dbAccessMode)
				throws SQLException, InterruptedException {
			final List<DbPathEntry> stack1 = getCompressionStack(entry1);
			if (stack1 == null) { // orphan
				if (dbAccessMode == CHECKEQUALITY_UPDATE || dbAccessMode == CHECKEQUALITY_AUTOSELECT) {
					deleteEquality(entry1.getPathId(), entry2.getPathId());
				}
				return false;
			}
			final List<DbPathEntry> stack2 = getCompressionStack(entry2);
			if (stack2 == null) { // orphan
				if (dbAccessMode == CHECKEQUALITY_UPDATE || dbAccessMode == CHECKEQUALITY_AUTOSELECT) {
					deleteEquality(entry1.getPathId(), entry2.getPathId());
				}
				return false;
			}
			return checkEquality(stack1, stack2, dbAccessMode);
		}

		public final int CHECKEQUALITY_NONE = 0;
		public final int CHECKEQUALITY_UPDATE = 1;
		public final int CHECKEQUALITY_INSERT = 2;
		public final int CHECKEQUALITY_AUTOSELECT = 3;
		public boolean checkEquality(
				final List<DbPathEntry> stack1,
				final List<DbPathEntry> stack2,
				final int dbAccessMode
				) throws SQLException, InterruptedException {
			if (stack1 == null || stack2 == null) { return false; /* orphan */ }
			DbPathEntry entry1 = stack1.get(0);
			DbPathEntry entry2 = stack2.get(0);

			Assertion.assertAssertionError(dbAccessMode >=0 && dbAccessMode <= 3);
			Assertion.assertAssertionError(entry1.isFile() || entry1.isCompressedFile(),
					"wrong type " + entry1.getType() + " for checkEquality: path=" + entry1.getPath());
			Assertion.assertAssertionError(entry2.isFile() || entry2.isCompressedFile(),
					"wrong type " + entry2.getType() + " for checkEquality: path=" + entry2.getPath());
			Assertion.assertAssertionError(entry1.getSize() == entry2.getSize());
			Assertion.assertAssertionError(!entry1.isCsumNull());
			Assertion.assertAssertionError(!entry2.isCsumNull());
			Assertion.assertAssertionError(entry1.getCsum() == entry2.getCsum());

			DbPathEntry re = null;
			long count=0L;
			try {
				boolean isEqual;
				re = entry1;
				InputStream stream1 = getInputStream(stack1);
				re = entry2;
				InputStream stream2 = getInputStream(stack2);
				re = null;

				try {
					for (;;) {
						re = entry1;
						int s1 = stream1.read();
						re = entry2;
						int s2 = stream2.read();
						re = null;
						if (s1 != s2) {
							isEqual = false;
							break;
						}
						if (s1 == -1 && s2 == -1) {
							isEqual = true;
							break;
						}
						count++;
					}
				} finally {
					re = entry1;
					stream1.close();
					re = entry2;
					stream2.close();
					re = null;
				}
				if (isEqual && count==entry1.getSize()) {
					if (dbAccessMode == CHECKEQUALITY_INSERT) {
						insertEquality(entry1.getPathId(), entry2.getPathId(), entry1.getSize(), entry1.getCsum());
					} else if (dbAccessMode == CHECKEQUALITY_UPDATE) {
						updateEquality(entry1.getPathId(), entry2.getPathId());
					} else if (dbAccessMode == CHECKEQUALITY_AUTOSELECT) {
						insertOrUpdateEquality(entry1.getPathId(), entry2.getPathId(), entry1.getSize(), entry1.getCsum());
					}
					if (entry1.isNoAccess()) {
						updateStatus(entry1, PathEntry.DIRTY);
					}
					if (entry2.isNoAccess()) {
						updateStatus(entry2, PathEntry.DIRTY);
					}
					return true;
				} else {
					if (!isEqual) {
						Debug.writelog("!! WARNING NOT EQUAL");
						if (dbAccessMode == CHECKEQUALITY_UPDATE || dbAccessMode == CHECKEQUALITY_AUTOSELECT) {
							deleteEquality(entry1.getPathId(), entry2.getPathId());
						}
					} else {
						Debug.writelog("!! EQUAL, BUT SIZE CHANGED "+entry1.getSize()+"->"+count);
					}

					Debug.writelog(entry1.getPath());
					re = entry1;
					unsetClean(entry1.getParentId());

					Debug.writelog(entry2.getPath());
					re = entry2;
					unsetClean(entry2.getParentId());

					re = null;
					return false;
				}
			} catch (IOException e) {
				if (re != null) {
					checkRootAndDisable(re);
				}
				if (dbAccessMode == CHECKEQUALITY_UPDATE || dbAccessMode == CHECKEQUALITY_AUTOSELECT) {
					deleteEquality(entry1.getPathId(), entry2.getPathId());
				}
				return false;
			}
		}
	}
}
