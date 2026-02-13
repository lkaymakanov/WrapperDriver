package com.is.util.db.driver.wrapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class WrapperCallableStatement extends WrapperPreparedStatement implements CallableStatement {

	@SuppressWarnings("unused")
	private FunctionParams 	mParams;
	private CallableStatement mCStmt;
	
	public WrapperCallableStatement(String aSql, CallableStatement aCStmt, 
			WrapperConnection aParentConnection, 
			 boolean aDebugMode) {
		super(aCStmt, aParentConnection, aSql,  aDebugMode);
		mCStmt = aCStmt;
		String funcName[] = detectFunctionName(aSql);
		boolean hasReturnParam = hasReturnParam(aSql);
		try {
			mParams = getFunctionParams(this, funcName[0], funcName[1], hasReturnParam);
		} catch (SQLException e) {
			//throw new RuntimeException(e);
		}
	}

	//===================== register out param ==========================
	public void registerOutParameter(int i, int j, String s)
			throws SQLException {
		mCStmt.registerOutParameter(i, j, s);
	}

	
	public void registerOutParameter(String s, int i) throws SQLException {
		mCStmt.registerOutParameter(s, i);
	}

	
	public void registerOutParameter(String s, int i, int j)
			throws SQLException {
		mCStmt.registerOutParameter(s, i, j);
	}

	
	public void registerOutParameter(String s, int i, String s1)
			throws SQLException {
		mCStmt.registerOutParameter(s, i, s1);
	}

	
	public void registerOutParameter(int i, int j) throws SQLException {
		mCStmt.registerOutParameter(i, j);
		bindVariableData.registerOutParameter(j, i);
	}

	
	public void registerOutParameter(int i, int j, int k) throws SQLException {
		mCStmt.registerOutParameter(i, j, k);
		bindVariableData.registerOutParameter(j, i);
	}
	//===================== register out param =============================
	
	
	public boolean wasNull() throws SQLException {
		return mCStmt.wasNull();
	}

	
	public String getString(int i) throws SQLException {
		return mCStmt.getString( i);
	}

	
	public boolean getBoolean(int i) throws SQLException {
		return mCStmt.getBoolean( i);
	}

	
	public byte getByte(int i) throws SQLException {
		return mCStmt.getByte( i);
	}

	
	public short getShort(int i) throws SQLException {
		return mCStmt.getShort( i);
	}

	
	public int getInt(int i) throws SQLException {
		return mCStmt.getInt( i);
	}

	
	public long getLong(int i) throws SQLException {
		return mCStmt.getLong( i);
	}

	
	public float getFloat(int i) throws SQLException {
		return mCStmt.getFloat( i);
	}

	
	public double getDouble(int i) throws SQLException {
		return mCStmt.getDouble( i);
	}

	
	public BigDecimal getBigDecimal(int i, int j) throws SQLException {
		return mCStmt.getBigDecimal( i);
	}

	
	public byte[] getBytes(int i) throws SQLException {
		return mCStmt.getBytes(i);
	}

	
	public Date getDate(int i) throws SQLException {
		return mCStmt.getDate( i);
	}

	
	public Time getTime(int i) throws SQLException {
		return mCStmt.getTime( i);
	}

	
	public Timestamp getTimestamp(int i) throws SQLException {
		return mCStmt.getTimestamp( i);
	}

	
	public Object getObject(int i) throws SQLException {
		return mCStmt.getObject(i);
	}

	
	public BigDecimal getBigDecimal(int i) throws SQLException {
		return mCStmt.getBigDecimal( i);
	}

	
	public Blob getBlob(int i) throws SQLException {
		return mCStmt.getBlob(i);
	}

	
	public Clob getClob(int i) throws SQLException {
		return mCStmt.getClob(i);
	}

	
	public Array getArray(int i) throws SQLException {
		return mCStmt.getArray(i);
	}

	
	public Date getDate(int i, Calendar calendar) throws SQLException {
		return mCStmt.getDate( i, calendar);
	}

	
	public Time getTime(int i, Calendar calendar) throws SQLException {
		return mCStmt.getTime(i, calendar);
	}

	
	public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
		return mCStmt.getTimestamp( i, calendar);
	}
	
	public URL getURL(int i) throws SQLException {
		return mCStmt.getURL(i);
	}

	
	public void setURL(String s, URL url) throws SQLException {
		mCStmt.setURL(s, url);
	}

	
	public void setNull(String s, int i) throws SQLException {
		mCStmt.setNull(s, i);
	}

	
	public void setBoolean(String s, boolean flag) throws SQLException {
		mCStmt.setBoolean(s, flag);
	}

	
	public void setByte(String s, byte byte0) throws SQLException {
		 mCStmt.setByte(s, byte0);
	}

	
	public void setShort(String s, short word0) throws SQLException {
		mCStmt.setShort(s, word0);
	}

	
	public void setInt(String s, int i) throws SQLException {
		mCStmt.setInt(s, i);
	}

	
	public void setLong(String s, long l) throws SQLException {
		mCStmt.setLong(s, l);
	}

	
	public void setFloat(String s, float f) throws SQLException {
		mCStmt.setFloat(s, f);
	}

	
	public void setDouble(String s, double d) throws SQLException {
		mCStmt.setDouble(s, d);
	}

	
	public void setBigDecimal(String s, BigDecimal bigdecimal)throws SQLException {
		mCStmt.setBigDecimal(s, bigdecimal);
	}

	
	public void setString(String s, String s1) throws SQLException {
		mCStmt.setString(s, s1);
	}

	
	public void setBytes(String s, byte[] abyte0) throws SQLException {
		mCStmt.setBytes(s, abyte0);
	}

	
	public void setDate(String s, Date date) throws SQLException {
		mCStmt.setDate(s, date);
	}

	
	public void setTime(String s, Time time) throws SQLException {
		mCStmt.setTime(s, time);
	}

	
	public void setTimestamp(String s, Timestamp timestamp) throws SQLException {
		mCStmt.setTimestamp(s, timestamp);
	}

	
	public void setAsciiStream(String s, InputStream inputstream, int i)
			throws SQLException {
		mCStmt.setAsciiStream(s, inputstream, i);
	}

	
	public void setBinaryStream(String s, InputStream inputstream, int i)
			throws SQLException {
		mCStmt.setBinaryStream(s, inputstream, i);
	}

	
	public void setObject(String s, Object obj, int i, int j)
			throws SQLException {
		mCStmt.setObject(s, obj, i, j);
	}

	
	public void setObject(String s, Object obj, int i) throws SQLException {
		mCStmt.setObject(s, obj, i);
	}

	
	public void setObject(String s, Object obj) throws SQLException {
		mCStmt.setObject(s, obj);
	}

	
	public void setCharacterStream(String s, Reader reader, int i) throws SQLException {
		mCStmt.setCharacterStream(s, reader, i);
	}

	
	public void setDate(String s, Date date, Calendar calendar)
			throws SQLException {
		mCStmt.setDate(s, date, calendar);
	}

	
	public void setTime(String s, Time time, Calendar calendar)
			throws SQLException {
		mCStmt.setTime(s, time, calendar);
	}

	
	public void setTimestamp(String s, Timestamp timestamp, Calendar calendar)
			throws SQLException {
		mCStmt.setTimestamp(s, timestamp, calendar);
	}

	
	public void setNull(String s, int i, String s1) throws SQLException {
		(mCStmt).setNull(s, i, s1);
	}

	
	public String getString(String s) throws SQLException {
		return mCStmt.getString( s);
	}

	
	public boolean getBoolean(String s) throws SQLException {
		return mCStmt.getBoolean(s);
	}

	
	public byte getByte(String s) throws SQLException {
		return mCStmt.getByte( s);
	}

	
	public short getShort(String s) throws SQLException {
		return mCStmt.getShort( s);
	}

	
	public int getInt(String s) throws SQLException {
		return mCStmt.getInt( s);
	}

	
	public long getLong(String s) throws SQLException {
		return mCStmt.getLong( s);
	}

	
	public float getFloat(String s) throws SQLException {
		return mCStmt.getFloat( s);
	}

	
	public double getDouble(String s) throws SQLException {
		return mCStmt.getDouble( s);
	}

	
	public byte[] getBytes(String s) throws SQLException {
		return (mCStmt).getBytes(s);
	}

	
	public Date getDate(String s) throws SQLException {
		return mCStmt.getDate( s);
	}

	
	public Time getTime(String s) throws SQLException {
		return mCStmt.getTime( s);
	}

	
	public Timestamp getTimestamp(String s) throws SQLException {
		return mCStmt.getTimestamp(s);
	}

	
	public Object getObject(String s) throws SQLException {
		return (mCStmt).getObject(s);
	}

	
	public BigDecimal getBigDecimal(String s) throws SQLException {
		return mCStmt.getBigDecimal( s);
	}

	
	public Ref getRef(String s) throws SQLException {
		return (mCStmt).getRef(s);
	}

	
	public Blob getBlob(String s) throws SQLException {
		return (mCStmt).getBlob(s);
	}

	
	public Clob getClob(String s) throws SQLException {
		return (mCStmt).getClob(s);
	}

	
	public Array getArray(String s) throws SQLException {
		return (mCStmt).getArray(s);
	}

	
	public Date getDate(String s, Calendar calendar) throws SQLException {
		return mCStmt.getDate(s, calendar);
	}

	
	public Time getTime(String s, Calendar calendar) throws SQLException {
		return mCStmt.getTime( s,calendar);
	}

	
	public Timestamp getTimestamp(String s, Calendar calendar)
			throws SQLException {
		return mCStmt.getTimestamp( s, calendar);
	}

	
	public URL getURL(String s) throws SQLException {
		return (mCStmt).getURL(s);
	}

	
	public RowId getRowId(int i) throws SQLException {
		return (mCStmt).getRowId(i);
	}

	
	public RowId getRowId(String s) throws SQLException {
		return (mCStmt).getRowId(s);
	}

	
	public void setRowId(String s, RowId rowid) throws SQLException {
		(mCStmt).setRowId(s, rowid);
	}

	
	public void setNString(String s, String s1) throws SQLException {
		mCStmt.setNString(s,  s1);
	}

	
	public void setNCharacterStream(String s, Reader reader, long l)
			throws SQLException {
		(mCStmt).setNCharacterStream(s, reader, l);
	}

	
	public void setNClob(String s, NClob nclob) throws SQLException {
		(mCStmt).setNClob(s, nclob);
	}

	
	public void setClob(String s, Reader reader, long l) throws SQLException {
		(mCStmt).setClob(s, reader, l);
	}

	
	public void setBlob(String s, InputStream inputstream, long l)
			throws SQLException {
		(mCStmt).setBlob(s, inputstream, l);
	}

	
	public void setNClob(String s, Reader reader, long l) throws SQLException {
		(mCStmt).setNClob(s, reader, l);
	}

	
	public NClob getNClob(int i) throws SQLException {
		return (mCStmt).getNClob(i);
	}

	
	public NClob getNClob(String s) throws SQLException {
		return (mCStmt).getNClob(s);
	}

	
	public void setSQLXML(String s, SQLXML sqlxml) throws SQLException {
		(mCStmt).setSQLXML(s, sqlxml);
	}

	
	public SQLXML getSQLXML(int i) throws SQLException {
		return (mCStmt).getSQLXML(i);
	}

	
	public SQLXML getSQLXML(String s) throws SQLException {
		return (mCStmt).getSQLXML(s);
	}

	
	public String getNString(int i) throws SQLException {
		return (mCStmt).getNString(i);
	}

	
	public String getNString(String s) throws SQLException {
		return (mCStmt).getNString(s);
	}

	
	public Reader getNCharacterStream(int i) throws SQLException {
		return (mCStmt).getNCharacterStream(i);
	}

	
	public Reader getNCharacterStream(String s) throws SQLException {
		return (mCStmt).getNCharacterStream(s);
	}

	
	public Reader getCharacterStream(int i) throws SQLException {
		return (mCStmt).getCharacterStream(i);
	}

	
	public Reader getCharacterStream(String s) throws SQLException {
		return (mCStmt).getCharacterStream(s);
	}

	
	public Object getObject(int parameterIndex, Map<String, Class<?>> map)
			throws SQLException {
		return (mCStmt).getObject(parameterIndex, map);
	}

	
	public Object getObject(String parameterName, Map<String, Class<?>> map)
			throws SQLException {
		return (mCStmt).getObject(parameterName, map);
	}

	
	public Ref getRef(int i) throws SQLException {
		return (mCStmt).getRef(i);
	}

	
	public void setBlob(String s, Blob blob) throws SQLException {
		(mCStmt).setBlob(s, blob);
	}

	
	public void setClob(String s, Clob clob) throws SQLException {
		(mCStmt).setClob(s, clob);
	}

	
	public void setAsciiStream(String s, InputStream inputstream, long l)
			throws SQLException {
		(mCStmt).setAsciiStream(s, inputstream, l);
	}

	
	public void setBinaryStream(String s, InputStream inputstream, long l)
			throws SQLException {
		(mCStmt).setBinaryStream(s, inputstream, l);
	}

	
	public void setCharacterStream(String s, Reader reader, long l)
			throws SQLException {
		(mCStmt).setCharacterStream(s, reader, l);
	}

	
	public void setAsciiStream(String s, InputStream inputstream)
			throws SQLException {
		(mCStmt).setAsciiStream(s, inputstream);
	}

	
	public void setBinaryStream(String s, InputStream inputstream)
			throws SQLException {
		(mCStmt).setBinaryStream(s, inputstream);
	}

	
	public void setCharacterStream(String s, Reader reader) throws SQLException {
		(mCStmt).setCharacterStream(s, reader);
	}

	
	public void setNCharacterStream(String s, Reader reader)
			throws SQLException {
		(mCStmt).setNCharacterStream(s, reader);
	}

	
	public void setClob(String s, Reader reader) throws SQLException {
		(mCStmt).setClob(s, reader);
	}

	
	public void setBlob(String s, InputStream inputstream) throws SQLException {
		mCStmt.setBlob(s, inputstream);
	}

	
	public void setNClob(String s, Reader reader) throws SQLException {
		mCStmt.setNClob(s, reader);
	}

	
	public void setBoolean(int arg0, boolean arg1) throws SQLException {
		super.setBoolean(arg0, arg1);
	}

	
	public void setByte(int arg0, byte arg1) throws SQLException {
		super.setByte(arg0, arg1);
	}

	
	public void setDate(int arg0, Date arg1) throws SQLException {
		super.setDate(arg0, arg1);
	}

	
	public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
		super.setDate(arg0, arg1, arg2);
	}

	
	public void setDouble(int arg0, double arg1) throws SQLException {
		super.setDouble(arg0, arg1);
	}

	
	public void setFloat(int arg0, float arg1) throws SQLException {
		super.setFloat(arg0, arg1);
	}

	
	public void setInt(int arg0, int arg1) throws SQLException {
		super.setInt(arg0, arg1);
	}

	
	public void setLong(int arg0, long arg1) throws SQLException {
		super.setLong(arg0, arg1);
	}

	
	public void setShort(int arg0, short arg1) throws SQLException {
		super.setShort(arg0, arg1);
	}

	
	public void setString(int arg0, String arg1) throws SQLException {
		super.setString(arg0, arg1);
	}

	
	public void setTime(int arg0, Time arg1) throws SQLException {
		super.setTime(arg0, arg1);
	}

	
	public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
		super.setTime(arg0, arg1, arg2);
	}

	
	public void setTimestamp(int arg0, Timestamp arg1) throws SQLException {
		super.setTimestamp(arg0, arg1);
	}

	
	public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2)
			throws SQLException {
		super.setTimestamp(arg0, arg1, arg2);
	}
	
	
	public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		super.setBigDecimal(arg0, arg1);
	}

	
	public void setNull(int arg0, int arg1) throws SQLException {
		super.setNull(arg0, arg1);
	}

	
	public void setNull(int paramIndex, int sqlType, String typeName)
			throws SQLException {
		super.setNull(paramIndex, sqlType, typeName);
	}

	
	public void setNString(int i, String s) throws SQLException {
		super.setNString(i, s);
	}

	
	public void setObject(int arg0, Object arg1) throws SQLException {
		super.setObject(arg0, arg1);
	}

	
	public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
		super.setObject(arg0, arg1, arg2);
	}

	
	public void setObject(int arg0, Object arg1, int arg2, int arg3)
			throws SQLException {
		super.setObject(arg0, arg1, arg2, arg3);
	}

	protected String[] detectFunctionName(String aSql) {
		String fullName = aSql.replaceFirst(Constants.CALL_STMT_BEGIN, "").replaceFirst(Constants.CALL_STMT_END, "");
		if (fullName.indexOf('.')<0) return new String[]{null, fullName};
		return new String[]{ fullName.substring(0, fullName.indexOf('.')), fullName.substring(fullName.indexOf('.')+1) };
	}
	
	protected boolean hasReturnParam(String aSql) {
		return aSql.matches(Constants.HAS_RETURN_PARAM_CALL_STMT_BEGIN);
	}


	@Override
	public <T> T getObject(int parameterIndex, Class<T> type)
			throws SQLException {
		return mCStmt.getObject(parameterIndex, type);
	}


	@Override
	public <T> T getObject(String parameterName, Class<T> type)
			throws SQLException {
		return mCStmt.getObject(parameterName, type);
	}
	
	
	private static FunctionParams getFunctionParams(Statement st, String schema, String procname, boolean hasReturnParam) throws SQLException {
		   
		FunctionParams result = null; 
		
	    DatabaseMetaData dbMetaData = st.getConnection().getMetaData();
	    ResultSet rs = dbMetaData.getProcedureColumns(st.getConnection().getCatalog(),null,null,null);
	    
	    while(rs.next()) {
	      // get stored procedure metadata
	      String procedureCatalog     = rs.getString(1);
	      String procedureSchema      = rs.getString(2);
	      String procedureName        = rs.getString(3);
	      String columnName           = rs.getString(4);
	      short  columnReturn         = rs.getShort(5);
	      int    columnDataType       = rs.getInt(6);
	      String columnReturnTypeName = rs.getString(7);
	      int    columnPrecision      = rs.getInt(8);
	      int    columnByteLength     = rs.getInt(9);
	      short  columnScale          = rs.getShort(10);
	      short  columnRadix          = rs.getShort(11);
	      short  columnNullable       = rs.getShort(12);
	      String columnRemarks        = rs.getString(13);

	      if ((schema==null || schema.equalsIgnoreCase(procedureSchema)) && (procedureName.equalsIgnoreCase(procname))) {
		      if (result==null) result=new FunctionParams(procedureCatalog, procedureSchema, procedureName);
		      
		      if (!(!hasReturnParam && columnReturn==5)) {
			      FunctionParam fp = new FunctionParam(columnName, columnReturn, columnDataType, columnReturnTypeName, columnPrecision, columnByteLength, columnScale, columnRadix, columnNullable, columnRemarks);
			      result.addParameter(fp);
		      }
	      }
	    }
	    
	    return result;
	}
	
	

}
