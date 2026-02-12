package com.is.util.db.driver.wrapper;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;



/**
 * The Class BindVariableData.
 */
class BindVariableDataDriver {
	
	/** The Constant DATE_FORMAT. */
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yyyy");
	
	/** The Constant TIMESTAMP_FORMAT. */
	private static final SimpleDateFormat TIMESTAMP_FORMAT  = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.SSS");
	
	/** The Constant TIME_FORMAT. */
	private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
	
	/** The db type. */
	
	/** The values. */
	private Map<Integer,BindVariableInfoDriver> values = new HashMap<Integer,BindVariableInfoDriver>();
	
	
	
	/**
	 * Instantiates a new bind variable data.
	 *
	 * @param v the v
	 */
	BindVariableDataDriver(Map<Integer, BindVariableInfoDriver> v){
		if(v != null){
			values = v;
		}
	}
	
	/**
	 * Instantiates a new bind variable data.
	 */
	public BindVariableDataDriver(){
		
	}
	
	public static BindVariableDataDriver getInstance(){
		return new BindVariableDataDriver();
	}
	

	
	/**
	 * Sets the int.
	 *
	 * @param val the val
	 * @param pos the pos
	 */
	public void setInt(Integer val,int pos){
		values.put(pos,new BindVariableInfoDriver(val, Types.INTEGER, pos));
	}
	
	/**
	 * Sets the int.
	 *
	 * @param val the val
	 * @return the int
	 */
	public int setInt(Integer val){
		int position = values.size()+1;
		values.put(position, new BindVariableInfoDriver(val, Types.INTEGER, position));
		return position;
	}
	
	/**
	 * Sets the string.
	 *
	 * @param str the str
	 * @param pos the pos
	 */
	public void setString(String str,int pos){
			values.put(pos,new BindVariableInfoDriver(str, Types.VARCHAR, pos));
	}
	
	/**
	 * Sets the long.
	 *
	 * @param val the val
	 * @param pos the pos
	 */
	public void setLong(Long val,int pos){
		values.put(pos,new BindVariableInfoDriver(val, Types.INTEGER, pos));
	}
	
	/**
	 * Sets the long.
	 *
	 * @param val the val
	 * @return the int
	 */
	public int setLong(Long val){
		int position = values.size()+1;
		values.put(position,new BindVariableInfoDriver(val, Types.BIGINT, position));
		return position;
	}
	
	/**
	 * Sets the big decimal.
	 *
	 * @param val the val
	 * @param pos the pos
	 */
	public void setBigDecimal(BigDecimal val,int pos){
		values.put(pos,new BindVariableInfoDriver(val, Types.DECIMAL, pos));
	}
	
	/**
	 * Sets the big decimal.
	 *
	 * @param val the val
	 * @return the int
	 */
	public int setBigDecimal(BigDecimal val){
		int position = values.size()+1;
		values.put(position,new BindVariableInfoDriver(val, Types.DECIMAL, position));
		return position;
	}
	
	/**
	 * Sets the date.
	 *
	 * @param val the val
	 * @param pos the pos
	 */
	public void setDate(Date val,int pos){
		values.put(pos,new BindVariableInfoDriver(val, Types.DATE, pos));
	}
	
	/**
	 * Sets the date.
	 *
	 * @param val the val
	 * @return the int
	 */
	public int setDate(Date val){
		int position = values.size()+1;
		values.put(position,new BindVariableInfoDriver(val, Types.DATE, position));
		return position;
	}
	
	/**
	 * Sets the time.
	 *
	 * @param val the val
	 * @param pos the pos
	 */
	public void setTime(Time val,int pos){
		values.put(pos,new BindVariableInfoDriver(val, Types.TIME, pos));
	}
	
	/**
	 * Sets the time.
	 *
	 * @param val the val
	 * @return the int
	 */
	public int setTime(Time val){
		int position = values.size()+1;
		values.put(position,new BindVariableInfoDriver(val, Types.TIME, position));
		return position;
	}
	
	/**
	 * Sets the timestamp.
	 *
	 * @param val the val
	 * @param pos the pos
	 */
	public void setTimestamp(Timestamp val,int pos){
		values.put(pos,new BindVariableInfoDriver(val, Types.TIMESTAMP, pos));
	}
	
	/**
	 * Sets the timestamp.
	 *
	 * @param val the val
	 * @return the int
	 */
	public int setTimestamp(Timestamp val){
		int position = values.size()+1;
		values.put(position,new BindVariableInfoDriver(val, Types.TIMESTAMP, position));
		return position;
	}
	
	/**
	 * Sets the string.
	 *
	 * @param str the str
	 * @return the int
	 */
	public int setString(String str){
		int position = values.size()+1;
		if (str != null && str.isEmpty())
			values.put(position, new BindVariableInfoDriver(null, Types.VARCHAR, position));
		else
			values.put(position, new BindVariableInfoDriver(str, Types.VARCHAR, position));
		return position;
	}
	
	
	/**
	 * Specify type explicitly with <code> setNull(int type)  </code>.
	 */
	@Deprecated
	public void setNull() {
		
		int position = values.size()+1;
		values.put(position,  new BindVariableInfoDriver(null, Types.NULL, position));
	}
	
	
	public void setBytes(byte [] bytes){
		int position = values.size()+1;
		values.put(position,  new BindVariableInfoDriver(bytes, Types.VARBINARY, position));
	}
	
	/**
	 * Sets the null.
	 *
	 * @param type the new null
	 */
	public void setNull(int type) {
		
		int position = values.size()+1;
		setNull(position, type);
	}
	
	public void setNull(int position, int type) {
		
		values.put(position,  new BindVariableInfoDriver(null, type, position));
	}
	
	public void setShort(short x){
		int position = values.size()+1;
		setShort(x, position);
	}
	
	public void setShort(short x, int parameterIndex) {
		
		values.put(parameterIndex,  new BindVariableInfoDriver(null, Types.INTEGER, parameterIndex));
	}
	
	/**
	 * Sets the float.
	 *
	 * @param f the new float
	 */
	public void setFloat(Float f, int position){
		//int position = values.size()+1;
		BigDecimal bd = null;
		if(f != null) bd = BigDecimal.valueOf(f);
		values.put(position,new BindVariableInfoDriver(bd, Types.DECIMAL, position));
	}
	
	public void setFloat(Float f){
		int position = values.size()+1;
		setFloat(f, position);
	}
	
	
	/**
	 * Sets the double.
	 *
	 * @param d the new double
	 */
	public void setDouble(Double d){
		int position = values.size()+1;
		setDouble(d, position);
	}
	

	public void setDouble(Double d, int position) {
		
		BigDecimal bd = null;
		if(d != null) bd = BigDecimal.valueOf(d);
		values.put(position,new BindVariableInfoDriver(bd, Types.DECIMAL, position));
	}
	
	/**
	 * Sets the boolean.
	 *
	 * @param b the new boolean
	 */
	public void setBoolean(boolean b){
		int position = values.size()+1;
		setBoolean(b, position);
	}
	
	
	public void setBooleanB(boolean b){
		int position = values.size()+1;
		setBooleanB(b, position);
	}
	
	
	public void setBoolean(boolean b, int position){
		if(b == true) values.put(position,new BindVariableInfoDriver(1, Types.NUMERIC, position));
		else  values.put(position,new BindVariableInfoDriver(0, Types.NUMERIC, position));
	}
	
	public void setBooleanB(boolean b, int position){
		 values.put(position,new BindVariableInfoDriver(b, Types.BOOLEAN, position));
		//else  values.put(position,new BindVariableInfo(0, Types.NUMERIC, position));
	}
	
	
	/**
	 * Sets the object.
	 *
	 * @param o the new object
	 */
	public void setObject(Object o){
		int position = values.size()+1;
		values.put(position,new BindVariableInfoDriver(o, Types.OTHER, position));
	}
	
	/**
	 * Sets the object.
	 *
	 * @param o the o
	 * @param pos the pos
	 */
	public void setObject(Object o,int pos){
		values.put(pos,new BindVariableInfoDriver(o, Types.OTHER, pos));
	}
	
	/**
	 * Register out parameter.
	 *
	 * @param type the type
	 */
	public void registerOutParameter(int type, int position){
		BindVariableInfoDriver infoObj = new BindVariableInfoDriver(new Object(), type, position, true);
		
		infoObj =  new BindVariableInfoDriver(new Object(),  Types.VARCHAR, position, true);
		
		values.put(position, infoObj);
	}
	
	public void registerOutParameter(int type){
		registerOutParameter(type, values.size()+1);
	}
	
	/**
	 * Sets the parameters.
	 *
	 * @param prStmt the pr stmt
	 * @return the prepared statement
	 * @throws SQLException the sQL exception
	 */
	public PreparedStatement setParameters(PreparedStatement prStmt) throws SQLException{
		for(BindVariableInfoDriver var:values.values()){
			if(var.getValue()!=null){
			    prStmt.setObject(var.getPosition(), var.getValue(), var.getType());
			}else{
				prStmt.setNull(var.getPosition(), var.getType());
			}
		}
		return prStmt;
	}
	
	

	/**
	 * Sets the parameters.
	 *
	 * @param callStmt the call stmt
	 * @return the callable statement
	 * @throws SQLException the sQL exception
	 */
	public CallableStatement setParameters(CallableStatement callStmt) throws SQLException{
		for(BindVariableInfoDriver var:values.values()){
			if(var.getValue()!=null){
				if(var.IsOutputParam() == true) callStmt.registerOutParameter(var.getPosition(), var.getType());
				else callStmt.setObject(var.getPosition(), var.getValue(), var.getType());
			}else{
				callStmt.setNull(var.getPosition(), var.getType());
			}
		}
		return callStmt;
	}
	
	/**
	 * Gets the clob as buffered reader.
	 *
	 * @param callStmt the call stmt
	 * @param paramIndex the param index
	 * @return the clob as buffered reader
	 */
	public BufferedReader getClobAsBufferedReader(CallableStatement callStmt, int paramIndex){
		CharArrayReader rdr;
		Clob arr = null;
		try{
			{
				Object obj = callStmt.getObject(paramIndex);
				String s  = obj!=null? obj.toString() : "";
				char [] charar = new char[s.length()];
				s.getChars(0, s.length(), charar, 0);
				rdr = new CharArrayReader(charar);
				return  new BufferedReader(rdr);
			}
			
		}catch (Exception e) {
			
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Gets the clob as buffered reader.
	 *
	 * @param rs the rs
	 * @param paramIndex the param index
	 * @return the clob as buffered reader
	 */
	public BufferedReader getClobAsBufferedReader(ResultSet rs, int paramIndex){
		CharArrayReader rdr;
		Clob arr = null;
		try{
			{
				Object obj = rs.getObject(paramIndex);
				String s  = obj!=null? obj.toString() : "";
				char [] charar = new char[s.length()];
				s.getChars(0, s.length(), charar, 0);
				rdr = new CharArrayReader(charar);
				return  new BufferedReader(rdr);
			}
			
		}catch (Exception e) {
			
			e.printStackTrace();
		}
		return null;
	}
	
	
	//TODO Wrap other setXXX methods in PreparedStatement
	
	/**
	 * Sql for log.
	 *
	 * @param sql the sql
	 * @return the string
	 */
	public String sqlForLog(String sql){
		if(sql!=null){
			char[] chars=sql.toCharArray();
			StringBuilder sb=new StringBuilder();
			for(int i=0,j=1;i<chars.length;i++){
				//TODO Handle question marks that are escaped
				if('?'==chars[i]){
					BindVariableInfoDriver pstmp=values.get(j);
					j++;
					if(pstmp!=null){
						sb.append(strForLog(pstmp));
						continue;
					}
				}
				sb.append(chars[i]);
			}
			return sb.toString();
		}
		return null;
	}
	
	

	/**
	 * Gets the values.
	 *
	 * @return the values
	 */
	public Map<Integer, BindVariableInfoDriver> getValues() {
		return values;
	}

	/**
	 * Str for log.
	 *
	 * @param aPstmp the a pstmp
	 * @return the string
	 */
	private String strForLog(BindVariableInfoDriver aPstmp) {
		if(aPstmp.getValue()!=null){
			switch(aPstmp.getType()){
			case Types.INTEGER:
			case Types.DOUBLE:
			case Types.BIGINT:
			case Types.DECIMAL:
			case Types.FLOAT:
				return ""+aPstmp.getValue();
			case Types.BOOLEAN:
				return (Boolean)aPstmp.getValue()?""+1:""+0;
			case Types.DATE:
				return "'"+DATE_FORMAT.format(aPstmp.getValue())+"'";
			case Types.TIMESTAMP:
				return "'"+TIMESTAMP_FORMAT.format(aPstmp.getValue())+"'";
			case Types.TIME:
				return "'"+TIME_FORMAT.format(aPstmp.getValue())+"'";
			case Types.CHAR:
			case Types.NCHAR:
			case Types.VARCHAR:
			case Types.NVARCHAR:
			case Types.LONGVARCHAR:
			case Types.LONGNVARCHAR:
				return "'"+aPstmp.getValue()+"'";
			default:
				return aPstmp.getValue().getClass().getName();
			}
		}
		return "null";
	}




}
