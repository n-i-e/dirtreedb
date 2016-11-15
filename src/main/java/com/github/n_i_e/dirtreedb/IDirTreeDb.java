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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public interface IDirTreeDb extends AutoCloseable {
	public abstract Statement createStatement() throws SQLException, InterruptedException;
	public abstract PreparedStatement prepareStatement(final String sql) throws SQLException, InterruptedException;
	public abstract void close() throws SQLException;
	public abstract DbPathEntry rsToPathEntry(ResultSet rs, String prefix) throws SQLException, InterruptedException;
	public abstract void insert(DbPathEntry basedir, PathEntry newentry) throws SQLException, InterruptedException;
	public abstract void update(DbPathEntry oldentry, PathEntry newentry) throws SQLException, InterruptedException;
	public abstract void updateStatus(DbPathEntry entry, int newstatus) throws SQLException, InterruptedException;
	public abstract void delete(DbPathEntry entry) throws SQLException, InterruptedException;
	public abstract void unsetClean(long pathid) throws SQLException, InterruptedException;
	public abstract void disable(DbPathEntry entry) throws SQLException, InterruptedException;
	public abstract void disable(DbPathEntry entry, PathEntry newentry) throws SQLException, InterruptedException;
	public abstract void updateParentId(DbPathEntry entry, long newparentid) throws SQLException, InterruptedException;
	public abstract void orphanize(DbPathEntry entry) throws SQLException, InterruptedException;
	public abstract void insertUpperLower(long upper, long lower, int distance) throws SQLException, InterruptedException;
	public abstract void deleteUpperLower(long upper, long lower) throws SQLException, InterruptedException;
	public abstract void insertEquality(long pathid1, long pathid2, long size, int csum) throws SQLException, InterruptedException;
	public abstract void deleteEquality(long pathid1, long pathid2) throws InterruptedException, SQLException;
	public abstract void updateEquality(long pathid1, long pathid2) throws InterruptedException, SQLException;
	public abstract void updateDuplicateFields(long pathid, long duplicate, long dedupablesize) throws InterruptedException, SQLException;

}
