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
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import com.github.n_i_e.dirtreedb.Assertion;

public class ResultSetWithDebugLog implements ResultSet {
	private ResultSet instance;
	private AbstractStatementWithDebugLog stmt;
	private int rowcount = 0;

	public ResultSetWithDebugLog(ResultSet originalResultSet, AbstractStatementWithDebugLog stmt) {
		instance = originalResultSet;
		this.stmt = stmt;
	}

	public static ResultSetWithDebugLog create(ResultSet originalResultSet,
			AbstractStatementWithDebugLog stmt) {
		if (originalResultSet == null) {
			return null;
		} else {
			return new ResultSetWithDebugLog(originalResultSet, stmt);
		}
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
	public boolean next() throws SQLException {
		boolean result;
		try {
			result = instance.next();
		} catch (SQLException e) {
			if (stmt != null) {
				e.printStackTrace();
				stmt.writelogQuery();
			}
			throw e;
		}
		rowcount++;
		if (result) {
			Assertion.assertAssertionError(instance.getRow() == rowcount, instance.getRow() + " != " + rowcount);
		}
		return result;
	}

	@Override
	public void close() throws SQLException {
		instance.close();
	}

	@Override
	public boolean wasNull() throws SQLException {
		return instance.wasNull();
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return instance.getString(columnIndex);
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return instance.getBoolean(columnIndex);
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return instance.getByte(columnIndex);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return instance.getShort(columnIndex);
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return instance.getInt(columnIndex);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return instance.getLong(columnIndex);
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return instance.getFloat(columnIndex);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return instance.getDouble(columnIndex);
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		return instance.getBigDecimal(columnIndex, scale);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		return instance.getBytes(columnIndex);
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return instance.getDate(columnIndex);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return instance.getTime(columnIndex);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return instance.getTimestamp(columnIndex);
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return instance.getAsciiStream(columnIndex);
	}

	@Override
	@Deprecated
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return instance.getUnicodeStream(columnIndex);
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return instance.getBinaryStream(columnIndex);
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return instance.getString(columnLabel);
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return instance.getBoolean(columnLabel);
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return instance.getByte(columnLabel);
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return instance.getShort(columnLabel);
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return instance.getInt(columnLabel);
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return instance.getLong(columnLabel);
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return instance.getFloat(columnLabel);
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return instance.getDouble(columnLabel);
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		return instance.getBigDecimal(columnLabel, scale);
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return instance.getBytes(columnLabel);
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return instance.getDate(columnLabel);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return instance.getTime(columnLabel);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return instance.getTimestamp(columnLabel);
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return instance.getAsciiStream(columnLabel);
	}

	@Override
	@Deprecated
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return instance.getUnicodeStream(columnLabel);
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return instance.getBinaryStream(columnLabel);
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
	public String getCursorName() throws SQLException {
		return instance.getCursorName();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return instance.getMetaData();
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return instance.getObject(columnIndex);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return instance.getObject(columnLabel);
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		return instance.findColumn(columnLabel);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		return instance.getCharacterStream(columnIndex);
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return instance.getCharacterStream(columnLabel);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return instance.getBigDecimal(columnIndex);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return instance.getBigDecimal(columnLabel);
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return instance.isBeforeFirst();
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return instance.isAfterLast();
	}

	@Override
	public boolean isFirst() throws SQLException {
		return instance.isFirst();
	}

	@Override
	public boolean isLast() throws SQLException {
		return instance.isLast();
	}

	@Override
	public void beforeFirst() throws SQLException {
		instance.beforeFirst();
	}

	@Override
	public void afterLast() throws SQLException {
		instance.afterLast();
	}

	@Override
	public boolean first() throws SQLException {
		return instance.first();
	}

	@Override
	public boolean last() throws SQLException {
		return instance.last();
	}

	@Override
	public int getRow() throws SQLException {
		return instance.getRow();
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		return instance.absolute(row);
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		return instance.relative(rows);
	}

	@Override
	public boolean previous() throws SQLException {
		return instance.previous();
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
	public int getType() throws SQLException {
		return instance.getType();
	}

	@Override
	public int getConcurrency() throws SQLException {
		return instance.getConcurrency();
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		return instance.rowUpdated();
	}

	@Override
	public boolean rowInserted() throws SQLException {
		return instance.rowInserted();
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		return instance.rowDeleted();
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		instance.updateNull(columnIndex);
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		instance.updateBoolean(columnIndex, x);
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		instance.updateByte(columnIndex, x);
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		instance.updateShort(columnIndex, x);
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		instance.updateInt(columnIndex, x);
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		instance.updateLong(columnIndex, x);
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		instance.updateFloat(columnIndex, x);
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		instance.updateDouble(columnIndex, x);
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		instance.updateBigDecimal(columnIndex, x);
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		instance.updateString(columnIndex, x);
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		instance.updateBytes(columnIndex, x);
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		instance.updateDate(columnIndex, x);
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		instance.updateTime(columnIndex, x);
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		instance.updateTimestamp(columnIndex, x);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		instance.updateAsciiStream(columnIndex, x, length);
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		instance.updateBinaryStream(columnIndex, x, length);
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		instance.updateCharacterStream(columnIndex, x, length);
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		instance.updateObject(columnIndex, x, scaleOrLength);
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		instance.updateObject(columnIndex, x);
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		instance.updateNull(columnLabel);
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		instance.updateBoolean(columnLabel, x);
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		instance.updateByte(columnLabel, x);
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		instance.updateShort(columnLabel, x);
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		instance.updateInt(columnLabel, x);
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		instance.updateLong(columnLabel, x);
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		instance.updateFloat(columnLabel, x);
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		instance.updateDouble(columnLabel, x);
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		instance.updateBigDecimal(columnLabel, x);
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		instance.updateString(columnLabel, x);
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		instance.updateBytes(columnLabel, x);
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		instance.updateDate(columnLabel, x);
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		instance.updateTime(columnLabel, x);
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		instance.updateTimestamp(columnLabel, x);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		instance.updateAsciiStream(columnLabel, x, length);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		instance.updateBinaryStream(columnLabel, x, length);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		instance.updateCharacterStream(columnLabel, reader, length);
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		instance.updateObject(columnLabel, x, scaleOrLength);
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		instance.updateObject(columnLabel, x);
	}

	@Override
	public void insertRow() throws SQLException {
		instance.insertRow();
	}

	@Override
	public void updateRow() throws SQLException {
		instance.updateRow();
	}

	@Override
	public void deleteRow() throws SQLException {
		instance.deleteRow();
	}

	@Override
	public void refreshRow() throws SQLException {
		instance.refreshRow();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		instance.cancelRowUpdates();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		instance.moveToInsertRow();
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		instance.moveToCurrentRow();
	}

	@Override
	public Statement getStatement() throws SQLException {
		return instance.getStatement();
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		return instance.getObject(columnIndex, map);
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		return instance.getRef(columnIndex);
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		return instance.getBlob(columnIndex);
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		return instance.getClob(columnIndex);
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		return instance.getArray(columnIndex);
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		return instance.getObject(columnLabel, map);
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		return instance.getRef(columnLabel);
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		return instance.getBlob(columnLabel);
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		return instance.getClob(columnLabel);
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		return instance.getArray(columnLabel);
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return instance.getDate(columnIndex, cal);
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return instance.getDate(columnLabel, cal);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return instance.getTime(columnIndex, cal);
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return instance.getTime(columnLabel, cal);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		return instance.getTimestamp(columnIndex, cal);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		return instance.getTimestamp(columnLabel, cal);
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		return instance.getURL(columnIndex);
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		return instance.getURL(columnLabel);
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		instance.updateRef(columnIndex, x);
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		instance.updateRef(columnLabel, x);
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		instance.updateBlob(columnIndex, x);
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		instance.updateBlob(columnLabel, x);
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		instance.updateClob(columnIndex, x);
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		instance.updateClob(columnLabel, x);
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		instance.updateArray(columnIndex, x);
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		instance.updateArray(columnLabel, x);
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		return instance.getRowId(columnIndex);
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		return instance.getRowId(columnLabel);
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		instance.updateRowId(columnIndex, x);
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		instance.updateRowId(columnLabel, x);
	}

	@Override
	public int getHoldability() throws SQLException {
		return instance.getHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return instance.isClosed();
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		instance.updateNString(columnIndex, nString);
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		instance.updateNString(columnLabel, nString);
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		instance.updateNClob(columnIndex, nClob);
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		instance.updateNClob(columnLabel, nClob);
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		return instance.getNClob(columnIndex);
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		return instance.getNClob(columnLabel);
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		return instance.getSQLXML(columnIndex);
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		return instance.getSQLXML(columnLabel);
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		instance.updateSQLXML(columnIndex, xmlObject);
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		instance.updateSQLXML(columnLabel, xmlObject);
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		return instance.getNString(columnIndex);
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return instance.getNString(columnLabel);
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return instance.getNCharacterStream(columnIndex);
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return instance.getNCharacterStream(columnLabel);
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		instance.updateNCharacterStream(columnIndex, x, length);
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		instance.updateNCharacterStream(columnLabel, reader, length);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		instance.updateAsciiStream(columnIndex, x, length);
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		instance.updateBinaryStream(columnIndex, x, length);
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		instance.updateCharacterStream(columnIndex, x, length);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		instance.updateAsciiStream(columnLabel, x, length);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		instance.updateBinaryStream(columnLabel, x, length);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		instance.updateCharacterStream(columnLabel, reader, length);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		instance.updateBlob(columnIndex, inputStream, length);
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		instance.updateBlob(columnLabel, inputStream, length);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		instance.updateClob(columnIndex, reader, length);
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		instance.updateClob(columnLabel, reader, length);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		instance.updateNClob(columnIndex, reader, length);
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		instance.updateNClob(columnLabel, reader, length);
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		instance.updateNCharacterStream(columnIndex, x);
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		instance.updateNCharacterStream(columnLabel, reader);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		instance.updateAsciiStream(columnIndex, x);
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		instance.updateBinaryStream(columnIndex, x);
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		instance.updateCharacterStream(columnIndex, x);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		instance.updateAsciiStream(columnLabel, x);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		instance.updateBinaryStream(columnLabel, x);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		instance.updateCharacterStream(columnLabel, reader);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		instance.updateBlob(columnIndex, inputStream);
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		instance.updateBlob(columnLabel, inputStream);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		instance.updateClob(columnIndex, reader);
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		instance.updateClob(columnLabel, reader);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		instance.updateNClob(columnIndex, reader);
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		instance.updateNClob(columnLabel, reader);

	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		return instance.getObject(columnIndex, type);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		return instance.getObject(columnLabel, type);
	}

}
