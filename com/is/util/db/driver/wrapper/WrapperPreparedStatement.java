
package com.is.util.db.driver.wrapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

public class WrapperPreparedStatement extends WrapperStatement implements PreparedStatement {

    protected PreparedStatement mPStmt;
    protected String mQuery;
    protected BindVariableDataDriver bindVariableData = new BindVariableDataDriver();

    public WrapperPreparedStatement(PreparedStatement aPStmt, WrapperConnection aParentConnection, String aQuery, boolean aDebugMode) {
        super(aPStmt, aParentConnection, aDebugMode);
        this.mPStmt = aPStmt;
        this.mQuery = aQuery;
    }
    // --------------------- Parameter Collection ---------------------

    @Override
    public void clearParameters() throws SQLException {
        bindVariableData.getValues().clear();
        mPStmt.clearParameters();
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        bindVariableData.setInt(x, parameterIndex);
        mPStmt.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        bindVariableData.setLong(x, parameterIndex);
        mPStmt.setLong(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        bindVariableData.setShort(x, parameterIndex);
        mPStmt.setShort(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        bindVariableData.setString(x, parameterIndex);
        mPStmt.setString(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        bindVariableData.setDouble(x, parameterIndex);
        mPStmt.setDouble(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        bindVariableData.setFloat(x, parameterIndex);
        mPStmt.setFloat(parameterIndex, x);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        bindVariableData.setBoolean(x, parameterIndex);
        mPStmt.setBoolean(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        bindVariableData.setBigDecimal(x, parameterIndex);
        mPStmt.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        bindVariableData.setDate(x, parameterIndex);
        mPStmt.setDate(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        bindVariableData.setDate(x, parameterIndex);
        mPStmt.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        bindVariableData.setTime(x, parameterIndex);
        mPStmt.setTime(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        bindVariableData.setTime(x, parameterIndex);
        mPStmt.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        bindVariableData.setTimestamp(x, parameterIndex);
        mPStmt.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        bindVariableData.setTimestamp(x, parameterIndex);
        mPStmt.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        bindVariableData.setNull(parameterIndex, sqlType);
        mPStmt.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        bindVariableData.setNull(parameterIndex, sqlType);
        mPStmt.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        bindVariableData.setBytes(x);
        mPStmt.setBytes(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        bindVariableData.setObject(reader, parameterIndex);
        mPStmt.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setArray(parameterIndex, x);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setURL(parameterIndex, x);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        bindVariableData.setString(value, parameterIndex);
        mPStmt.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        bindVariableData.setObject(value, parameterIndex);
        mPStmt.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        bindVariableData.setObject(value, parameterIndex);
        mPStmt.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        bindVariableData.setObject(reader, parameterIndex);
        mPStmt.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        bindVariableData.setObject(inputStream, parameterIndex);
        mPStmt.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        bindVariableData.setObject(reader, parameterIndex);
        mPStmt.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        bindVariableData.setObject(xmlObject, parameterIndex);
        mPStmt.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        bindVariableData.setObject(reader, parameterIndex);
        mPStmt.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        bindVariableData.setObject(x, parameterIndex);
        mPStmt.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        bindVariableData.setObject(reader, parameterIndex);
        mPStmt.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        bindVariableData.setObject(reader, parameterIndex);
        mPStmt.setNCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        bindVariableData.setObject(reader, parameterIndex);
        mPStmt.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        bindVariableData.setObject(inputStream, parameterIndex);
        mPStmt.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        bindVariableData.setObject(reader, parameterIndex);
        mPStmt.setNClob(parameterIndex, reader);
    }

    // --------------------- Execution Logging ---------------------

    @Override
    public boolean execute() throws SQLException {
        long start = System.nanoTime();
        boolean result = mPStmt.execute();
        log(start);
        return result;
    }

    @Override
    public int executeUpdate() throws SQLException {
    	long start = System.nanoTime();
        int rows = mPStmt.executeUpdate();
        log(start);
        return rows;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
    	long start = System.nanoTime();
        ResultSet rs = mPStmt.executeQuery();
        log(start);
        return new WrapperResultSet(rs, this, mDebugMode);
    }
    
    private void log(long start) {
   	    long elapsed = (System.nanoTime() - start) / 1_000_000;
        if (mDebugMode){
            String sqlForLog = bindVariableData.sqlForLog(mQuery);
            WrapperDriver.logFunction.accept(sqlForLog + " | executed in " + elapsed + "ms");
        }
        bindVariableData.getValues().clear();
   }

    // --------------------- Delegate Remaining Methods ---------------------
    @Override public void addBatch() throws SQLException { mPStmt.addBatch(); }
    @Override public ResultSetMetaData getMetaData() throws SQLException { return mPStmt.getMetaData(); }
    @Override public ParameterMetaData getParameterMetaData() throws SQLException { return mPStmt.getParameterMetaData(); }
    //@Override public PreparedStatement getWrappedPreparedStatement() { return mPStmt; }

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		mPStmt.setByte(parameterIndex, x);
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		mPStmt.setRef(parameterIndex, x);
	}
}
