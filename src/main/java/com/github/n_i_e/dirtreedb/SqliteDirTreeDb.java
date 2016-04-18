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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SqliteDirTreeDb extends CommonSqlDirTreeDb {
	static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	SqliteDirTreeDb(String filename) throws SQLException, ClassNotFoundException {
		Class.forName("org.sqlite.JDBC");

		File fileobj = new File(filename);
		boolean fileExists = fileobj.exists();

		conn = DriverManager.getConnection("jdbc:sqlite:" + filename);
		conn.setAutoCommit(true);
		Statement stmt = conn.createStatement();
		try {
			stmt.execute("PRAGMA synchronous=OFF;");
			stmt.execute("PRAGMA journal_mode=WAL;");
			stmt.execute("PRAGMA foreign_keys=ON;");
			//stmt.execute("PRAGMA case_sensitive_like=ON;");

			if (!fileExists) {
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS directory (pathid INTEGER PRIMARY KEY AUTOINCREMENT, "
						+ "parentid INTEGER NOT NULL, rootid INTEGER, datelastmodified TEXT NOT NULL, "
						+ "size INTEGER NOT NULL, compressedsize INTEGER NOT NULL, csum INTEGER, "
						+ "path TEXT UNIQUE NOT NULL, type INTEGER NOT NULL, status INTEGER NOT NULL, "
						+ "duplicate INTEGER NOT NULL, dedupablesize INTEGER NOT NULL, "
						+ "CONSTRAINT pathid_size_csum UNIQUE (pathid, size, csum))");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_parentid ON directory (parentid)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_rootid ON directory (rootid)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_datelastmodified ON directory (datelastmodified)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_size ON directory (size)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_compressedsize ON directory (compressedsize)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_csum ON directory (csum)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_size_csum ON directory (size, csum)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_type ON directory (type)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_status ON directory (status)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_duplicate ON directory (duplicate)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_dedupablesize ON directory (dedupablesize)");

				stmt.executeUpdate("INSERT OR IGNORE INTO directory (path, parentid, datelastmodified, size, "
						+ "compressedsize, type, status, duplicate, dedupablesize) "
						+ "VALUES ('C:\\', 0, '2000-01-01 00:00:00', 0, 0, 0, 1, 0, 0)");

				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS upperlower (upper INTEGER NOT NULL, "
						+ "lower INTEGER NOT NULL, distance INTEGER NOT NULL, PRIMARY KEY (upper, lower), "
						+ "FOREIGN KEY (upper) REFERENCES directory (pathid), "
						+ "FOREIGN KEY (lower) REFERENCES directory (pathid))");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS upperlower_distance ON upperlower (distance)");

				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS equality ("
						+ "pathid1 INTEGER NOT NULL, pathid2 INTEGER NOT NULL, "
						+ "size INTEGER NOT NULL, csum INTEGER NOT NULL, "
						+ "datelasttested TEXT NOT NULL, PRIMARY KEY (pathid1, pathid2), "
						+ "FOREIGN KEY (pathid1, size, csum) REFERENCES directory (pathid, size, csum), "
						+ "FOREIGN KEY (pathid2, size, csum) REFERENCES directory (pathid, size, csum))");
			}
			stmt.executeUpdate("UPDATE directory SET rootid=pathid WHERE rootid IS NULL AND pathid IS NOT NULL");
		} finally {
			//stmt.executeUpdate("VACUUM");
			stmt.close();
		}
	}

	@Override
	public DbPathEntry rsToPathEntry(ResultSet rs, String prefix) throws SQLException, InterruptedException
	{
		if (rs == null) {
			return null;
		}
		try {
			if (rs.rowDeleted()) {
				return null;
			}
		} catch (SQLFeatureNotSupportedException e) {}

		long newpathid = rs.getLong(prefix + "pathid");
		long newparentid = rs.getLong(prefix + "parentid");
		long newrootid = rs.getLong(prefix + "rootid");
		if (rs.wasNull()) {
			newrootid = newpathid;
		}

		String newpath = rs.getString(prefix + "path");
		if (rs.wasNull()) {
			return null;
		}

		int newtype = rs.getInt(prefix + "type");

		DbPathEntry result = new DbPathEntry(newpath, newtype, newpathid, newparentid, newrootid);

		String t = rs.getString(prefix + "datelastmodified");
		if (t == null) {
			result.setDateLastModified(0);
		} else {
			Date d;
			try {
				d = sdf.parse(t);
				result.setDateLastModified(d.getTime());
			} catch (ParseException e) {
				result.setDateLastModified(0);
			}
		}

		result.setSize(rs.getLong(prefix + "size"));
		result.setCompressedSize(rs.getLong(prefix + "compressedsize"));

		int newcsum = rs.getInt(prefix + "csum");
		if (!rs.wasNull()) {
			result.setCsum(newcsum);
		}

		result.setStatus(rs.getInt(prefix + "status"));

		return result;
	}

	@Override
	public void insert(DbPathEntry basedir, PathEntry newentry) throws SQLException, InterruptedException
	{
		String sql = null;
		try {
			if (basedir == null) { // root folder
				PreparedStatement ps;
				sql = "INSERT INTO directory (parentid, datelastmodified, size, compressedsize, path, type, status, duplicate, dedupablesize) VALUES (0, ?, 0, 0, ?, 0, 1, 0, 0)";
				ps = conn.prepareStatement(sql);
				ps.setTimestamp(1, new Timestamp(new Date().getTime()));
				ps.setString(2, newentry.getPath());
				ps.executeUpdate();

				sql = "UPDATE directory SET rootid=pathid WHERE rootid IS NULL AND pathid IS NOT NULL";
				ps = conn.prepareStatement(sql);
				ps.executeUpdate();
				ps.close();
			} else {
				assert(basedir.getPath().equals(newentry.getPath().substring(0, basedir.getPath().length())));
				PreparedStatement ps;
				if (newentry.isCsumNull()) {
					sql = "INSERT INTO directory (parentid, rootid, datelastmodified, size, compressedsize, path, type, status, duplicate, dedupablesize) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, 0)";
					ps = conn.prepareStatement(sql);
				} else {
					sql = "INSERT INTO directory (parentid, rootid, datelastmodified, size, compressedsize, path, type, status, duplicate, dedupablesize, csum) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, 0, ?)";
					ps = conn.prepareStatement(sql);
					ps.setInt(8, newentry.getCsum());
				}
				ps.setLong(1, basedir.getPathId());
				ps.setLong(2, basedir.getRootId());
				Date d = new Date(newentry.getDateLastModified());
				ps.setString(3, sdf.format(d));
				ps.setLong(4, newentry.getSize());
				ps.setLong(5, newentry.getCompressedSize());
				ps.setString(6, newentry.getPath());
				ps.setInt(7, newentry.getType());
				ps.executeUpdate();
				ps.close();
			}
		} catch (SQLException e) {
			if (sql != null) {
				writelog("!! SQL insert failed at CommonSqlDirTreeDB for: " + sql);
				writelog("newentry.path = " + newentry.getPath());
				if (basedir != null) {
					writelog("basedir.path = " + basedir.getPath());
				}
			}
			throw e;
		}
	}

	@Override
	public void update(DbPathEntry oldentry, PathEntry newentry) throws SQLException, InterruptedException
	{
		assert(oldentry.getPath().equals(newentry.getPath()));

		PreparedStatement ps;
		if (newentry.isCsumNull()) {
			ps = conn.prepareStatement("UPDATE directory SET datelastmodified=?, size=?, compressedsize=?, status=?, csum=NULL WHERE pathid=?");
			ps.setLong(5, oldentry.getPathId());
		} else {
			ps = conn.prepareStatement("UPDATE directory SET datelastmodified=?, size=?, compressedsize=?, status=?, csum=? WHERE pathid=?");
			ps.setLong(5, newentry.getCsum());
			ps.setLong(6, oldentry.getPathId());
		}
		Date d = new Date(newentry.getDateLastModified());
		ps.setString(1, sdf.format(d));
		ps.setLong(2, newentry.getSize());
		ps.setLong(3, newentry.getCompressedSize());
		ps.setInt(4, newentry.getStatus());
		ps.executeUpdate();
		ps.close();
	}

	@Override
	public void insertEquality(long pathid1, long pathid2, long size, int csum)
			throws SQLException, InterruptedException {
		PreparedStatement ps = prepareStatement("INSERT INTO equality (pathid1, pathid2, size, csum, datelasttested) VALUES (?, ?, ?, ?, ?)");
		try {
			if (pathid1>pathid2) {
				ps.setLong(1, pathid1);
				ps.setLong(2, pathid2);
			} else {
				ps.setLong(1, pathid2);
				ps.setLong(2, pathid1);
			}
			ps.setLong(3, size);
			ps.setInt(4, csum);
			Date d = new Date();
			ps.setString(5, sdf.format(d));
			ps.executeUpdate();
		} catch (SQLException e) {
			writelog("SQLException at addEquality: pathid1=" + pathid1 + ", pathid2=" + pathid2);
			throw e;
		} finally {
			ps.close();
		}
	}

	@Override
	public void updateEquality(long pathid1, long pathid2) throws InterruptedException, SQLException {
		PreparedStatement ps = prepareStatement("UPDATE equality SET datelasttested=? WHERE pathid1=? AND pathid2=?");
		try {
			Date d = new Date();
			ps.setString(1, sdf.format(d));
			if (pathid1>pathid2) {
				ps.setLong(2, pathid1);
				ps.setLong(3, pathid2);
			} else {
				ps.setLong(2, pathid2);
				ps.setLong(3, pathid1);
			}
			ps.executeUpdate();
		} finally {
			ps.close();
		}
	}
}
