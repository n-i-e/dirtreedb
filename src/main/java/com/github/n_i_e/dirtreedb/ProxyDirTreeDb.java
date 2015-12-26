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

public class ProxyDirTreeDb extends AbstractDirTreeDb {
	protected AbstractDirTreeDb parent;

	ProxyDirTreeDb (AbstractDirTreeDb parent) {
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
			writelog("threadHookInterval too long: " + d);
			if (n - _threadHookInterval > 15*60*1000) {
				StackTraceElement[] st = Thread.currentThread().getStackTrace();
				for (int c = 0; c < st.length; c++) {
					writelog(st[c].toString());
				}
			}
		}
		_threadHookInterval = n;
		Thread.sleep(0); // throw InterruptedException if interrupted
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

	@Override
	public void insert(DbPathEntry basedir, PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(newentry != null);
		threadHook();
		try {
			parent.insert(basedir, newentry);
		} catch (SQLException e) {
			cleanupOrphans_NoOverride(newentry.getPath());
			parent.insert(basedir, newentry);
		}
	}

	@Override
	public void update(DbPathEntry oldentry, PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(oldentry != null);
		Assertion.assertNullPointerException(newentry != null);
		Assertion.assertAssertionError(oldentry.getPath().equals(newentry.getPath()),
				"!! old and new entry paths do not match:\nold=" + oldentry.getPath() + "\nnew=" + newentry.getPath());
		assert(oldentry.getType() == newentry.getType());
		
		if (! dscMatch(oldentry, newentry)
				|| oldentry.isCsumNull() != newentry.isCsumNull()
				|| (!oldentry.isCsumNull() && !newentry.isCsumNull() && oldentry.getCsum() != newentry.getCsum())
				) {
			deleteEquality_NoOverride(oldentry.getPathId());
			threadHook();
			parent.update(oldentry, newentry);
		} else if (oldentry.getStatus() != newentry.getStatus()) {
			threadHook();
			parent.update(oldentry, newentry);
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

	private void delete_NoOverride(DbPathEntry entry) throws SQLException, InterruptedException {
		deleteEquality_NoOverride(entry.getPathId());
		deleteUpperLower_NoOverride(entry.getPathId());
		threadHook();
		parent.delete(entry);
	}

	@Override
	public void delete(DbPathEntry entry) throws SQLException, InterruptedException {
		delete_NoOverride(entry);
	}

	protected void delete_LowPriority(DbPathEntry entry) throws SQLException, InterruptedException {
		delete_NoOverride(entry);
	}

	public void delete(Iterator<DbPathEntry> entries) throws SQLException, InterruptedException {
		while (entries.hasNext()) {
			delete(entries.next());
		}
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

	private void deleteEquality_NoOverride(long pathid) throws InterruptedException, SQLException {
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

	public void deleteEquality(long pathid) throws InterruptedException, SQLException {
		deleteEquality_NoOverride(pathid);
	}

	private void deleteUpperLower_NoOverride(long pathid) throws SQLException, InterruptedException {
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

	public void deleteUpperLower(long pathid) throws SQLException, InterruptedException {
		deleteUpperLower_NoOverride(pathid);
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

	public File getFileIfExists(final DbPathEntry entry) throws SQLException, InterruptedException {
		assert(entry.getType() == PathEntry.FOLDER || entry.getType() == PathEntry.FILE);
		threadHook();

		File result = new File(entry.getPath());
		if (!result.exists()
				|| (entry.getType() == PathEntry.FOLDER && !result.isDirectory())
				|| (entry.getType() == PathEntry.FILE && !result.isFile())
				) {
			if (!entry.isRoot()) {
				// basedir does not exist
				final DbPathEntry parent;
				parent = getParent(entry);
				if (parent == null) {
					return null; // orphan
				} else if (!parent.isDirty()) {
					updateStatus(parent, PathEntry.DIRTY);
				}
			}
			updateStatus(entry, PathEntry.NOACCESS);
			return null;
		}
		return result;
	}

	public HashMap<String, DbPathEntry> childrenList(DbPathEntry entry) throws SQLException, InterruptedException {
		threadHook();

		HashMap<String, DbPathEntry> result = new HashMap<String, DbPathEntry>();

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
			Assertion.assertAssertionError(entry.isFile());
			InputStream result = new PathEntry(entry).getInputStream();

			for (int i=stack.size()-2; i>=0; i--) {
				DbPathEntry parent = entry;
				InputStream parentStream = result;
				entry = stack.get(i);
				Assertion.assertAssertionError(entry.isCompressedFile(),
						"wrong element " + stack.get(i).getPath() + ", type=" + entry.getType());
				IDirArchiveLister z = ArchiveListerFactory.getArchiveLister(parent, parentStream);
				result = z.getInputStream(entry);
				Assertion.assertAssertionError(result != null);
			}
			return result;
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

	public DbPathEntry getDbPathEntryByPath(String pathid) throws SQLException, InterruptedException {
		String sql = "SELECT * from DIRECTORY where PATH=?";
		PreparedStatement ps = prepareStatement(sql);
		ps.setString(1, pathid);
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				DbPathEntry p = rsToPathEntry(rs);
				if (p.getPath().equals(pathid)) {
					return p;
				}
			}
		} finally {
			rs.close();
			ps.close();
		}
		return null;
	}

	/**
	 * An orphan entry is a DIRECTORY entry with invalid PARENTID (there is no row with PATHID of that number).
	 */
	private int cleanupOrphans(String path, boolean noOverride) throws SQLException, InterruptedException {
		PreparedStatement ps;
		if (path == null) {
			String sql = "SELECT * FROM directory AS d1 WHERE parentid>0 "
					+ "AND NOT EXISTS (SELECT * FROM directory AS d2 WHERE d1.parentid=d2.pathid)";
			ps = prepareStatement(sql);
		} else {
			String sql = "SELECT * FROM directory AS d1 WHERE parentid>0 AND path=? "
					+ "AND NOT EXISTS (SELECT * FROM directory AS d2 WHERE d1.parentid=d2.pathid)";
			ps = prepareStatement(sql);
			ps.setString(1, path);
		}
		try {
			ResultSet rs = ps.executeQuery();
			try {
				int count = 0;
				while (rs.next()) {
					threadHook();
					if (noOverride) {
						writelog("cleanupOrphans_NoLazy: deleting " + rsToPathEntry(rs).getPath());
						delete_NoOverride(rsToPathEntry(rs));
					} else {
						delete_LowPriority(rsToPathEntry(rs));
					}
					count++;
				}
				return count;
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
	}

	private int cleanupOrphans_NoOverride(String path) throws SQLException, InterruptedException {
		return cleanupOrphans(path, true);
	}

	public int cleanupOrphans(String path) throws SQLException, InterruptedException {
		return cleanupOrphans(path, false);
	}

	public int cleanupOrphans() throws SQLException, InterruptedException {
		return cleanupOrphans(null);
	}

	public void cleanupOrphansAll() throws SQLException, InterruptedException {
		while (cleanupOrphans() > 0) {}
	}

	public void refreshDirectUpperLower() throws SQLException, InterruptedException {
		refreshDirectUpperLower(null);
	}

	public void refreshDirectUpperLower(List<Long> dontListRootIds) throws SQLException, InterruptedException {
		threadHook();
		Statement stmt = createStatement();
		try {
			threadHook();
			ResultSet rs = stmt.executeQuery("SELECT parentid, pathid FROM directory AS d1 WHERE parentid>0 "
					+ getDontListRootIdsSubSql(dontListRootIds)
					+ "AND EXISTS (SELECT * FROM directory WHERE pathid=d1.parentid) " // NOT orphan
					+ "AND NOT EXISTS (SELECT * FROM upperlower WHERE distance=1 AND parentid=upper AND pathid=lower)");
			try {
				while (rs.next()) {
					threadHook();
					insertUpperLower(rs.getLong("parentid"), rs.getLong("pathid"), 1);
				}
			} finally {
				rs.close();
			}
		} finally {
			stmt.close();
		}
	}

	public void refreshIndirectUpperLower() throws SQLException, InterruptedException {
		refreshIndirectUpperLower(null);
	}

	public void refreshIndirectUpperLower(List<Long> dontListRootIds) throws SQLException, InterruptedException {
		Statement stmt = createStatement();
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
				}
			} finally {
				rs.close();
			}
		} finally {
			stmt.close();
		}
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

	public void refreshFolderSizesAll() throws SQLException, InterruptedException {
		threadHook();
		while (refreshFolderSizes() > 0) {}
	}

	public int refreshFolderSizes() throws SQLException, InterruptedException {
		Statement stmt = createStatement();
		ResultSet rs = stmt.executeQuery("SELECT d1.*, newsize, newcompressedsize FROM "
				+ "(SELECT * FROM directory WHERE type=0) as d1, "
				+ "(SELECT parentid, SUM(size) AS newsize, SUM(compressedsize) AS newcompressedsize FROM directory "
				+ "WHERE parentid IS NOT NULL AND parentid > 0 AND size IS NOT NULL AND compressedsize IS NOT NULL "
				+ "GROUP BY parentid) AS d2 "
				+ "WHERE d1.pathid=d2.parentid "
				+ "AND ((d1.size IS NULL OR d1.size<>d2.newsize) "
				+ "OR (d1.compressedsize IS NULL OR d1.compressedsize<>d2.newcompressedsize))");
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
			}
			return count;
		} finally {
			rs.close();
			stmt.close();
		}
	}

	public void refreshDuplicateFields_OBSOLETE() throws InterruptedException, SQLException {
		threadHook();

		Statement stmt = createStatement();
		try {
			{
				ResultSet rs = stmt.executeQuery("SELECT pathid FROM directory "
						+ "WHERE (duplicate<>0 OR dedupablesize<>0) "
						+ "AND NOT EXISTS (SELECT * FROM equality WHERE pathid=pathid1 OR pathid=pathid2)");
				try {
					while (rs.next()) {
						threadHook();
						updateDuplicateFields(rs.getLong("pathid"), 0, 0);
					}
				} finally {
					rs.close();
				}
			}

			threadHook();
			{
				ResultSet rs = stmt.executeQuery("SELECT pathid, newduplicate, newduplicate*size as newdedupablesize "
						+ "FROM directory, "
						+ "(SELECT newpathid, SUM(newduplicate) AS newduplicate FROM ("
						+ "SELECT pathid1 AS newpathid, count(pathid2) AS newduplicate FROM equality GROUP BY pathid1 "
						+ "UNION ALL "
						+ "SELECT pathid2 AS newpathid, count(pathid1) AS newduplicate FROM equality GROUP BY pathid2"
						+ ") GROUP BY newpathid) "
						+ "WHERE pathid=newpathid AND (duplicate<>newduplicate OR dedupablesize<>newduplicate*size)");
				try {
					while (rs.next()) {
						threadHook();
						updateDuplicateFields(rs.getLong("pathid"), rs.getLong("newduplicate"), rs.getLong("newdedupablesize"));
					}
				} finally {
					rs.close();
				}
			}
		} finally {
			stmt.close();
		}
	}

	public void refreshDuplicateFields() throws InterruptedException, SQLException {
		threadHook();

		Statement stmt1 = createStatement();
		try {
			ResultSet rs = stmt1.executeQuery("SELECT pathid, newduplicate, newdedupablesize "
					+ "FROM directory AS d1, "
					+ "(SELECT size, csum, count(size)-1 AS newduplicate, (count(size)-1)*size AS newdedupablesize "
					+ "FROM directory WHERE (type=1 OR type=3) AND CSUM IS NOT NULL GROUP BY size, csum "
					+ "HAVING (COUNT(size)>=2 AND COUNT(size)-1<>MAX(duplicate)) "
					+ "OR (COUNT(size)>=2 AND COUNT(size)-1<>MAX(duplicate)) "
					+ "OR MAX(duplicate)>MIN(duplicate)) AS d2 "
					+ "WHERE (d1.type=1 OR d1.type=3) AND d1.size=d2.size AND d1.csum=d2.csum");
			try {
				while (rs.next()) {
					threadHook();
					updateDuplicateFields(rs.getLong("pathid"), rs.getLong("newduplicate"), rs.getLong("newdedupablesize"));
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
				}
			} finally {
				rs.close();
			}
		} finally {
			stmt2.close();
		}
	}

	public static File getFileIfExists(final PathEntry entry) throws SQLException {
		assert(entry.getType() == PathEntry.FOLDER || entry.getType() == PathEntry.FILE);

		File result = new File(entry.getPath());
		if (!result.exists()
				|| (entry.getType() == PathEntry.FOLDER && !result.isDirectory())
				|| (entry.getType() == PathEntry.FILE && !result.isFile())
				) {
			// basedir does not exist
			return null;
		}
		return result;
	}

	public static PathEntry getNewPathEntry(final PathEntry entry) throws SQLException, IOException {
		if (entry.isFolder() || entry.isFile()) {
			final File fileobj = getFileIfExists(entry);
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
				if (dMatch(entry, result)) {
					result.setStatus(entry.getStatus());
				}
				
			} else { // isFile
				if (dscMatch(entry, result)) {
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
		writelog("This is SingleThread Dispatcher");
		return new Dispatcher();
	}

	public class Dispatcher extends AbstractDirTreeDb.Dispatcher {

		@Override
		public PathEntry dispatch(final DbPathEntry entry) throws IOException, InterruptedException, SQLException {
			threadHook();
			if (entry == null) {
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
				disable(entry);
				return null;
			}

			final PathEntry newentry;
			try {
				newentry = new PathEntry(fileobj);
			} catch (IOException e) {
				disable(entry);
				return null;
			}

			newentry.setSize(entry.getSize());
			newentry.setCompressedSize(entry.getCompressedSize());

			if (entry.isClean() && dMatch(entry, newentry)) {
				return newentry; // no change
			}

			try {
				final DirLister newfolderIter;
				if (isList()) {
					newfolderIter = new DirLister(entry, fileobj);
				} else {
					newfolderIter = null;
				}

				final HashMap<String, DbPathEntry> oldfolder;
				if (!isList() || newfolderIter == null) {
					oldfolder = null;
				} else if (isNoChildInDb()) {
					oldfolder = new HashMap<String, DbPathEntry>();
				} else {
					oldfolder = childrenList(entry);
				}

				if (!isList()) {
					if (!entry.isDirty() && !dMatch(entry, newentry)) {
						updateStatus(entry, PathEntry.DIRTY);
					}
				} else if (isList() && newfolderIter != null && oldfolder != null) {
					dispatchFolderListCore(entry, fileobj, oldfolder, newentry, newfolderIter);
				}
			} catch (IOException e) {
				disable(entry);
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
				disable(entry);
				return null;
			}

			if (dscMatch(entry, newentry)) {
				if (!entry.isCsumNull()) {
					newentry.setCsum(entry.getCsum());
				}
				newentry.setStatus(entry.getStatus());
			}

			try {
				final IArchiveLister newfolderIter;
				if (isList() && (!entry.isClean() || !dscMatch(entry, newentry))) {
					newfolderIter = ArchiveListerFactory.getArchiveLister(entry, newentry.getInputStream());
				} else {
					newfolderIter = null;
				}

				final HashMap<String, DbPathEntry> oldfolder;
				if (!isList() || newfolderIter == null) {
					oldfolder = null;
				} else if (isNoChildInDb()) {
					oldfolder = new HashMap<String, DbPathEntry>();
				} else {
					oldfolder = childrenList(entry);
				}

				if (oldfolder == null) { // not isList()
					if (!entry.isDirty() && !dscMatch(entry, newentry)) {
						newentry.setStatus(PathEntry.DIRTY);
					}
				} else {
					assert(newfolderIter != null);
					dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
				}
				if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !dscMatch(entry, newentry)))) {
					newentry.setCsumAndClose(newentry.getInputStream());
					if (newentry.isNoAccess()) {
						newentry.setStatus(PathEntry.DIRTY);
					}
				}
				update(entry, newentry);
			} catch (IOException e) {
				disable(entry, newentry);
			} catch (OutOfMemoryError e) {
				writelog("!! WARNING !! Caught OutOfMemoryError at: " + entry.getPath());
				e.printStackTrace();
				disable(entry, newentry);
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
					final IArchiveLister newfolderIter;
					if (!entry.isClean()) {
						newfolderIter = ArchiveListerFactory.getArchiveLister(entry, getInputStream(stack));
					} else {
						newfolderIter = null;
					}

					final HashMap<String, DbPathEntry> oldfolder;
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

				if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !dscMatch(entry, newentry)))) {
					assert(stack != null);
					newentry.setCsumAndClose(getInputStream(stack));
					if (newentry.isNoAccess()) {
						newentry.setStatus(PathEntry.DIRTY);
					}
				}
				update(entry, newentry);
			} catch (IOException e) {
				disable(entry);
			} catch (OutOfMemoryError e) {
				writelog("!! WARNING !! Caught OutOfMemoryError at: " + entry.getPath());
				e.printStackTrace();
				disable(entry, newentry);
			}
			return newentry;
		}

		protected void dispatchFolderListCore(
				DbPathEntry entry,
				File fileobj,
				HashMap<String, DbPathEntry> oldfolder,
				PathEntry newentry,
				DirLister newfolderIter
				) throws InterruptedException, SQLException, IOException {
			long new_size = 0;
			long new_compressedsize = 0;

			final List<DbPathEntry> updatedfolders = new ArrayList<DbPathEntry>();
			ArrayList<String> uniqueChecker = new ArrayList<String>();
			while (newfolderIter.hasNext()) {
				PathEntry newchild = newfolderIter.next();
				Assertion.assertAssertionError(newchild.isFolder() || newchild.isFile());
				if (oldfolder.containsKey(newchild.getPath())) { // exists in oldfolder - update
					DbPathEntry oldchild = oldfolder.get(newchild.getPath());
					if (newchild.isFolder()) {
						if (!oldchild.isDirty() && !dMatch(oldchild, newchild)) {
							updatedfolders.add(oldchild);
						}
						new_size += oldchild.getSize();
						new_compressedsize += oldchild.getCompressedSize();
					} else { // FILE
						if (isListCsumForce() || (isListCsum() && (oldchild.isCsumNull() || !dscMatch(oldchild, newchild)))) {
							try {
								newchild.setCsumAndClose(newchild.getInputStream());
							} catch (IOException e) {
								newchild.setStatus(PathEntry.NOACCESS);
							}
							update(oldchild, newchild);
						} else if (!dscMatch(oldchild, newchild)) {
							update(oldchild, newchild);
						}
						new_size += newchild.getSize();
						new_compressedsize += newchild.getCompressedSize();
					}
				} else { // not in oldfolder - insert
					if (isListCsum() && newchild.isFile()) {
						try {
							newchild.setCsumAndClose(newchild.getInputStream());
						} catch (IOException e) {
							newchild.setStatus(PathEntry.NOACCESS);
						}
					}
					if (uniqueChecker.contains(newchild.getPath())) {
						writelog("!! warning !! dispatchFileListCore found duplicate: " + newchild.getPath());
					} else {
						insert(entry, newchild);
					}
					if (newchild.isFile()) {
						new_size += newchild.getSize();
						new_compressedsize += newchild.getCompressedSize();
					}
				}
				oldfolder.remove(newchild.getPath());
			}

			updateStatuses(updatedfolders.iterator(), PathEntry.DIRTY);
			delete(oldfolder.values().iterator());

			newentry.setSize(new_size);
			newentry.setCompressedSize(new_compressedsize);
			newentry.setStatus(PathEntry.CLEAN);
			update(entry, newentry);
		}

		protected void dispatchFileListCore(
				DbPathEntry entry,
				HashMap<String, DbPathEntry> oldfolder,
				PathEntry newentry,
				IArchiveLister newfolderIter
				) throws InterruptedException, SQLException, IOException {
			ArrayList<String> uniqueChecker = new ArrayList<String>();
			while (newfolderIter.hasNext(true)) {
				PathEntry newchild = newfolderIter.next(true);
				Assertion.assertAssertionError(newchild.isCompressedFolder() || newchild.isCompressedFile());
				if (oldfolder.containsKey(newchild.getPath())) {
					DbPathEntry oldchild = oldfolder.get(newchild.getPath());
					if (!dscMatch(oldchild, newchild)) {
						update(oldchild, newchild);
					}
				} else {
					if (uniqueChecker.contains(newchild.getPath())) {
						writelog("!! warning !! dispatchFileListCore found duplicate: " + newchild.getPath());
					} else {
						insert(entry, newchild);
					}
				}
				oldfolder.remove(newchild.getPath());
			}
			newfolderIter.close();
			delete(oldfolder.values().iterator());
			newentry.setStatus(PathEntry.CLEAN);
			update(entry, newentry);
		}

		public boolean checkEquality(final DbPathEntry entry1, final DbPathEntry entry2, final int dbAccessMode)
				throws SQLException, InterruptedException {
			final List<DbPathEntry> stack1 = getCompressionStack(entry1);
			if (stack1 == null) { return false; /* orphan */ }
			final List<DbPathEntry> stack2 = getCompressionStack(entry2);
			if (stack2 == null) { return false; /* orphan */ }
			return checkEquality(stack1, stack2, dbAccessMode);
		}

		public final static int CHECKEQUALITY_NONE = 0;
		public final static int CHECKEQUALITY_UPDATE = 1;
		public final static int CHECKEQUALITY_INSERT = 2;
		public final static int CHECKEQUALITY_AUTOSELECT = 3;
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
						writelog("!! WARNING NOT EQUAL");
					} else {
						writelog("!! EQUAL, BUT SIZE CHANGED "+entry1.getSize()+"->"+count);
					}
					writelog(entry1.getPath());
					writelog(entry2.getPath());
					if (dbAccessMode == CHECKEQUALITY_UPDATE || dbAccessMode == CHECKEQUALITY_AUTOSELECT) {
						deleteEquality(entry1.getPathId(), entry2.getPathId());
					}
					re = entry1;
					PathEntry newentry1 = getNewPathEntry(entry1);
					stream1 = getInputStream(stack1);
					newentry1.setCsumAndClose(stream1);
					if (newentry1.isNoAccess()) {
						newentry1.setStatus(PathEntry.DIRTY);
					}
					update(entry1, newentry1);

					re = entry2;
					PathEntry newentry2 = getNewPathEntry(entry2);
					stream2 = getInputStream(stack2);
					newentry2.setCsumAndClose(stream2);
					if (newentry2.isNoAccess()) {
						newentry2.setStatus(PathEntry.DIRTY);
					}
					update(entry2, newentry2);
					re = null;
					return false;
				}
			} catch (IOException e) {
				if (re != null) {
					disable(re);
				}
				return false;
			} catch (OutOfMemoryError e) {
				writelog("!! OutOfMemory at ProxyDirTreeDb re=" + re.getPath());
				throw e;
			}
		}
	}
}
