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

package com.github.n_i_e.dirtreedb.debug;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;

public class PreparedStatementWithDebugLog extends AbstractStatementWithDebugLog implements PreparedStatement {
	private PreparedStatement instance;
	private HashMap<Integer, String> parameter = new HashMap<Integer, String>();

	public PreparedStatementWithDebugLog(PreparedStatement originalPreparedStatement) {
		instance = originalPreparedStatement;
	}

	public PreparedStatementWithDebugLog(PreparedStatement originalPreparedStatement, String sql) {
		instance = originalPreparedStatement;
		this.sql = sql;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		this.sql = sql;
		long t1 = (new java.util.Date()).getTime();
		try {
			return ResultSetWithDebugLog.create(instance.executeQuery(sql), this);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		this.sql = sql;
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.executeUpdate(sql);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public void close() throws SQLException {
		instance.close();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return instance.getMaxFieldSize();
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		instance.setMaxFieldSize(max);
	}

	@Override
	public int getMaxRows() throws SQLException {
		return instance.getMaxRows();
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		instance.setMaxRows(max);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		instance.setEscapeProcessing(enable);
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return instance.getQueryTimeout();
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		instance.setQueryTimeout(seconds);
	}

	@Override
	public void cancel() throws SQLException {
		instance.cancel();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return instance.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		instance.clearWarnings();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		instance.setCursorName(name);
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		this.sql = sql;
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.execute(sql);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return ResultSetWithDebugLog.create(instance.getResultSet(), this);
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return instance.getUpdateCount();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return instance.getMoreResults();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		instance.setFetchDirection(direction);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return instance.getFetchDirection();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		instance.setFetchSize(rows);
	}

	@Override
	public int getFetchSize() throws SQLException {
		return instance.getFetchSize();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return instance.getResultSetConcurrency();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return instance.getResultSetType();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		this.sql = sql;
		instance.addBatch();
	}

	@Override
	public void clearBatch() throws SQLException {
		instance.clearBatch();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.executeBatch();
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public Connection getConnection() throws SQLException {
		return instance.getConnection();
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return instance.getMoreResults();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return ResultSetWithDebugLog.create(instance.getGeneratedKeys(), this);
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		this.sql = sql;
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.executeUpdate(sql, autoGeneratedKeys);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		this.sql = sql;
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.executeUpdate(sql, columnIndexes);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		this.sql = sql;
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.executeUpdate(sql, columnNames);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		this.sql = sql;
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.execute(sql, autoGeneratedKeys);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		this.sql = sql;
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.execute(sql, columnIndexes);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		this.sql = sql;
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.execute(sql, columnNames);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return instance.getResultSetHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return instance.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		instance.setPoolable(poolable);
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return instance.isPoolable();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		instance.closeOnCompletion();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return instance.isCloseOnCompletion();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return instance.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return instance.isWrapperFor(iface);
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		long t1 = (new java.util.Date()).getTime();
		try {
			return ResultSetWithDebugLog.create(instance.executeQuery(), this);
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public int executeUpdate() throws SQLException {
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.executeUpdate();
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		instance.setNull(parameterIndex, sqlType);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		parameter.put(new Integer(parameterIndex), x ? "true" : "false");
		instance.setBoolean(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		parameter.put(new Integer(parameterIndex), String.valueOf(x));
		instance.setByte(parameterIndex, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		parameter.put(new Integer(parameterIndex), String.valueOf(x));
		instance.setShort(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		parameter.put(new Integer(parameterIndex), String.valueOf(x));
		instance.setInt(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		parameter.put(new Integer(parameterIndex), String.valueOf(x));
		try {
			instance.setLong(parameterIndex, x);
		} catch (SQLException e) {
			writelog("SQLException caught at setLong: parameterIndex=" + parameterIndex + " vaule=" + x);
			writelogQuery();
			throw e;
		}
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		parameter.put(new Integer(parameterIndex), String.valueOf(x));
		instance.setFloat(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		parameter.put(new Integer(parameterIndex), String.valueOf(x));
		instance.setDouble(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setBigDecimal(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setString(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setBytes(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setDate(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setTime(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setTimestamp(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setAsciiStream(parameterIndex, x, length);
	}

	@Deprecated
	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setUnicodeStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void clearParameters() throws SQLException {
		instance.clearParameters();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSQLType)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setObject(parameterIndex, x, targetSQLType);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setObject(parameterIndex, x);
	}

	@Override
	public boolean execute() throws SQLException {
		long t1 = (new java.util.Date()).getTime();
		try {
			return instance.execute();
		} finally {
			long dt = (new java.util.Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt);
			}
		}
	}

	@Override
	public void addBatch() throws SQLException {
		instance.addBatch();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		instance.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setRef(parameterIndex, x);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setBlob(parameterIndex, x);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setClob(parameterIndex, x);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setArray(parameterIndex, x);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return instance.getMetaData();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setDate(parameterIndex, x, cal);
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setTime(parameterIndex, x, cal);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setTimestamp(parameterIndex, x, cal);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		instance.setNull(parameterIndex, sqlType, typeName);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setURL(parameterIndex, x);
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return instance.getParameterMetaData();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setRowId(parameterIndex, x);
	}

	@Override
	public void setNString(int parameterIndex, String value)
			throws SQLException {
		instance.setNString(parameterIndex, value);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		instance.setNCharacterStream(parameterIndex, value, length);
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		instance.setNClob(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		instance.setClob(parameterIndex, reader, length);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		instance.setBlob(parameterIndex, inputStream, length);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		instance.setNClob(parameterIndex, reader, length);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		instance.setSQLXML(parameterIndex, xmlObject);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSQLType,
			int scaleOrLength) throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setObject(parameterIndex, x, targetSQLType, scaleOrLength);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setAsciiStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		instance.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setAsciiStream(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		if (x != null) { parameter.put(new Integer(parameterIndex), x.toString()); }
		instance.setBinaryStream(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		instance.setCharacterStream(parameterIndex, reader);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		instance.setNCharacterStream(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		instance.setClob(parameterIndex, reader);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		instance.setBlob(parameterIndex, inputStream);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		instance.setNClob(parameterIndex, reader);
	}

	protected void writelog(long dtime) {
		writelog("PreparedStatement execution too long: " + dtime + " msec");
		writelogQuery();
	}

	public void writelogQuery() {
		writelog(sql);
		ArrayList<Integer> keylist = new ArrayList<Integer>(parameter.keySet());
		Collections.sort(keylist);
		for (int key: keylist) {
			writelog(key + " " + parameter.get(key));
		}
	}

}
