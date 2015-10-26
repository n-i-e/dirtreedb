package com.github.n_i_e.dirtreedb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Date;

public class StatementWithSlowQueryLog implements Statement {
	private Statement instance;


	public StatementWithSlowQueryLog(Statement originalStatement) {
		instance = originalStatement;
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
	public ResultSet executeQuery(String sql) throws SQLException {
		long t1 = (new Date()).getTime();
		try {
			return ResultSetWithIntegrityCheck.create(instance.executeQuery(sql));
		} finally {
			long dt = (new Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt, sql);
			}
		}
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		long t1 = (new Date()).getTime();
		try {
			return instance.executeUpdate(sql);
		} finally {
			long dt = (new Date()).getTime() - t1;
			if (dt > 30*1000) {
				writelog(dt, sql);
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
		long t1 = (new Date()).getTime();
		try {
			return instance.execute(sql);
		} finally {
			long dt = (new Date()).getTime() - t1;
			if (dt > 30*1000) {
				System.out.println("Statement execution too long: " + dt + " msec\n" + sql);
			}
		}
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return ResultSetWithIntegrityCheck.create(instance.getResultSet());
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
		instance.addBatch(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		instance.clearBatch();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		return instance.executeBatch();
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
		return ResultSetWithIntegrityCheck.create(instance.getGeneratedKeys());
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		int result;
		long t1 = (new Date()).getTime();
		try {
			result = instance.executeUpdate(sql, autoGeneratedKeys);
		} finally {
			long dt = (new Date()).getTime() - t1;
			if (dt > 30*1000) {
				System.out.println("Statement execution too long: " + dt + " msec\n" + sql);
			}
		}
		return result;
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		int result;
		long t1 = (new Date()).getTime();
		try {
			result = instance.executeUpdate(sql, columnIndexes);
		} finally {
			long dt = (new Date()).getTime() - t1;
			if (dt > 30*1000) {
				System.out.println("Statement execution too long: " + dt + " msec\n" + sql);
			}
		}
		return result;
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		int result;
		long t1 = (new Date()).getTime();
		try {
			result = instance.executeUpdate(sql, columnNames);
		} finally {
			long dt = (new Date()).getTime() - t1;
			if (dt > 30*1000) {
				System.out.println("Statement execution too long: " + dt + " msec\n" + sql);
			}
		}
		return result;

	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		boolean result;
		long t1 = (new Date()).getTime();
		try {
			result = instance.execute(sql, autoGeneratedKeys);
		} finally {
			long dt = (new Date()).getTime() - t1;
			if (dt > 30*1000) {
				System.out.println("Statement execution too long: " + dt + " msec\n" + sql);
			}
		}
		return result;
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		boolean result;
		long t1 = (new Date()).getTime();
		try {
			result = instance.execute(sql, columnIndexes);
		} finally {
			long dt = (new Date()).getTime() - t1;
			if (dt > 30*1000) {
				System.out.println("Statement execution too long: " + dt + " msec\n" + sql);
			}
		}
		return result;
	}

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		boolean result;
		long t1 = (new Date()).getTime();
		try {
			result = instance.execute(sql, columnNames);
		} finally {
			long dt = (new Date()).getTime() - t1;
			if (dt > 30*1000) {
				System.out.println("Statement execution too long: " + dt + " msec\n" + sql);
			}
		}
		return result;
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

	protected static void writelog(long dtime, String sql) {
		writelog("Statement execution too long: " + dtime + " msec");
		writelog(sql);
	}

	protected static void writelog(final String message) {
		Date now = new Date();
		System.out.print(now);
		System.out.print(" ");
		System.out.println(message);
	}
}
