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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;

public abstract class CommonSqlDirTreeDb extends AbstractDirTreeDb {
	Connection conn;

	public void close() throws SQLException {
		conn.close();
	}

	public Statement createStatement() throws SQLException, InterruptedException {
		return new StatementWithDebugLog(conn.createStatement());
	}

	public PreparedStatement prepareStatement(final String sql) throws SQLException, InterruptedException {
		return new PreparedStatementWithDebugLog(conn.prepareStatement(sql), sql);
	}

	public DbPathEntry rsToPathEntry(ResultSet rs, String prefix) throws SQLException, InterruptedException {
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

		result.setDateLastModified(rs.getTimestamp(prefix + "datelastmodified").getTime());
		result.setSize(rs.getLong(prefix + "size"));
		result.setCompressedSize(rs.getLong(prefix + "compressedsize"));

		int newcsum = rs.getInt(prefix + "csum");
		if (!rs.wasNull()) {
			result.setCsum(newcsum);
		}

		result.setStatus(rs.getInt(prefix + "status"));

		return result;
	}

	public void insert(DbPathEntry basedir, PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(newentry != null);
		String sql = null;
		try {
			if (basedir == null) { // root folder
				PreparedStatement ps;
				sql = "INSERT INTO directory (parentid, datelastmodified, size, compressedsize, path, type, status, duplicate, dedupablesize) VALUES (0, ?, 0, 0, ?, 0, 1, 0, 0)";
				ps = prepareStatement(sql);
				ps.setTimestamp(1, new Timestamp(new Date().getTime()));
				ps.setString(2, newentry.getPath());
				ps.executeUpdate();

				sql = "UPDATE directory SET rootid=pathid WHERE rootid IS NULL AND pathid IS NOT NULL";
				ps = prepareStatement(sql);
				ps.executeUpdate();
				ps.close();
			} else {
				assert(basedir.getPath().equals(newentry.getPath().substring(0, basedir.getPath().length())));
				PreparedStatement ps;
				if (newentry.isCsumNull()) {
					sql = "INSERT INTO directory (parentid, rootid, datelastmodified, size, compressedsize, path, type, status, duplicate, dedupablesize) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, 0)";
					ps = prepareStatement(sql);
				} else {
					sql = "INSERT INTO directory (parentid, rootid, datelastmodified, size, compressedsize, path, type, status, duplicate, dedupablesize, csum) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, 0, ?)";
					ps = prepareStatement(sql);
					ps.setInt(8, newentry.getCsum());
				}
				ps.setLong(1, basedir.getPathId());
				ps.setLong(2, basedir.getRootId());
				ps.setTimestamp(3, new Timestamp(newentry.getDateLastModified()));
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

	public void update(DbPathEntry oldentry, PathEntry newentry) throws SQLException, InterruptedException
	{
		Assertion.assertAssertionError(oldentry.getPath().equals(newentry.getPath()));

		PreparedStatement ps;
		if (newentry.isCsumNull()) {
			ps = prepareStatement("UPDATE directory SET "
					+ "datelastmodified=?, size=?, compressedsize=?, status=?,  csum=NULL WHERE pathid=?");
			ps.setLong(5, oldentry.getPathId());
		} else {
			ps = prepareStatement("UPDATE directory SET "
					+ "datelastmodified=?, size=?, compressedsize=?, status=?, csum=? WHERE pathid=?");
			ps.setLong(5, newentry.getCsum());
			ps.setLong(6, oldentry.getPathId());
		}
		ps.setTimestamp(1, new Timestamp(newentry.getDateLastModified()));
		ps.setLong(2, newentry.getSize());
		ps.setLong(3, newentry.getCompressedSize());
		ps.setInt(4, newentry.getStatus());
		ps.executeUpdate();
		ps.close();
	}

	public void updateStatus(DbPathEntry entry, int newstatus) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertAssertionError(newstatus == PathEntry.CLEAN
				|| newstatus == PathEntry.DIRTY
				|| newstatus == PathEntry.NOACCESS);
		String sql = "UPDATE directory SET status=? WHERE pathid=?";
		PreparedStatement ps = prepareStatement(sql);
		try {
			ps.setInt(1, newstatus);
			ps.setLong(2, entry.getPathId());
			ps.executeUpdate();
		} finally {
			ps.close();
		}
	}

	public void delete(DbPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		PreparedStatement ps = prepareStatement("DELETE FROM directory WHERE pathid=?");
		try {
			ps.setLong(1, entry.getPathId());
			ps.executeUpdate();
		} finally {
			ps.close();
		}
	}

	public void disable(DbPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		PreparedStatement ps;
		ps = prepareStatement("UPDATE directory SET status=1 WHERE pathid=? AND status=0");
		try {
			ps.setLong(1, entry.getParentId());
			ps.executeUpdate();
		} finally {
			ps.close();
		}

		ps = prepareStatement("UPDATE directory SET status=2 WHERE pathid=? AND status<2");
		try {
			ps.setLong(1, entry.getPathId());
			ps.executeUpdate();
		} finally {
			ps.close();
		}
	}

	public void disable(DbPathEntry entry, PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertNullPointerException(newentry != null);
		PreparedStatement ps;
		ps = prepareStatement("UPDATE directory SET status=1 WHERE pathid=? AND status=0");
		try {
			ps.setLong(1, entry.getParentId());
			ps.executeUpdate();
		} finally {
			ps.close();
		}
		newentry.setStatus(PathEntry.NOACCESS);
		update(entry, newentry);
	}

	@Override
	public void insertUpperLower(long upper, long lower, int distance) throws SQLException, InterruptedException {
		PreparedStatement ps;
		ps = prepareStatement("INSERT INTO upperlower (upper, lower, distance) VALUES (?, ?, ?)");
		ps.setLong(1, upper);
		ps.setLong(2, lower);
		ps.setLong(3, distance);
		try {
			ps.executeUpdate();
		} catch (SQLException e) {
			writelog("SQLException at addUpperLower: upper=" + upper + ", lower=" + lower);
			throw e;
		} finally {
			ps.close();
		}
	}

	@Override
	public void deleteUpperLower(long upper, long lower) throws SQLException, InterruptedException {
		PreparedStatement ps;
		String sql = "DELETE FROM upperlower WHERE upper=? AND lower=?";
		ps = prepareStatement(sql);
		try {
			ps.setLong(1, upper);
			ps.setLong(2, lower);
			ps.executeUpdate();
		} finally {
			ps.close();
		}
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
			ps.setTimestamp(5, new Timestamp(new Date().getTime()));
			ps.executeUpdate();
		} catch (SQLException e) {
			writelog("SQLException at addEquality: pathid1=" + pathid1 + ", pathid2=" + pathid2);
			throw e;
		} finally {
			ps.close();
		}
	}

	@Override
	public void deleteEquality(long pathid1, long pathid2) throws InterruptedException, SQLException {
		PreparedStatement ps = prepareStatement("DELETE FROM equality WHERE pathid1=? AND pathid2=?");
		try {
			if (pathid1>pathid2) {
				ps.setLong(1, pathid1);
				ps.setLong(2, pathid2);
			} else {
				ps.setLong(1, pathid2);
				ps.setLong(2, pathid1);
			}
			ps.executeUpdate();
		} finally {
			ps.close();
		}
	}

	@Override
	public void updateEquality(long pathid1, long pathid2) throws InterruptedException, SQLException {
		PreparedStatement ps = prepareStatement("UPDATE equality SET datelasttested=? WHERE pathid1=? AND pathid2=?");
		try {
			ps.setTimestamp(1, new Timestamp(new Date().getTime()));
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

	@Override
	public void updateDuplicateFields(long pathid, long duplicate, long dedupablesize) throws InterruptedException, SQLException {
		PreparedStatement ps = prepareStatement("UPDATE directory SET duplicate=?, dedupablesize=? WHERE pathid=?");
		try {
			ps.setLong(1, duplicate);
			ps.setLong(2, dedupablesize);
			ps.setLong(3, pathid);
			ps.executeUpdate();
		} finally {
			ps.close();
		}
	}
}
