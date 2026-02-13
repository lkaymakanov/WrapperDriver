package com.is.util.db.driver.wrapper;


/**
 * The Class BindVariableInfo.
 */
class BindVariableInfoDriver {
	
	/** The value. */
	private Object 	value;
	
	/** The type. */
	private int		type;
	
	/** The position. */
	private int 	position;
	
	/** The outputparam. */
	private boolean outputparam = false;
	
	private boolean setInPreparedStatement = false;

	/**
	 * Instantiates a new bind variable info.
	 *
	 * @param value the value
	 * @param type the type
	 * @param position the position
	 */
	public BindVariableInfoDriver(Object value,int type,int position){
		this.value=value;
		this.type=type;
		this.position=position;
		this.outputparam = false;
		
	}
	
	/**
	 * Instantiates a new bind variable info.
	 *
	 * @param value the value
	 * @param type the type
	 * @param position the position
	 * @param output the output
	 */
	public BindVariableInfoDriver(Object value,int type,int position, boolean output){
		this.value=value;
		this.type=type;
		this.position=position;
		this.outputparam = output;
	}

	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public Object getValue() {
		
		return value;
	}

	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	public int getType() {
		
		return type;
	}

	/**
	 * Gets the position.
	 *
	 * @return the position
	 */
	public int getPosition() {
		
		return position;
	}

	/**
	 * Checks if is output param.
	 *
	 * @return true, if successful
	 */
	public boolean IsOutputParam() {
		
		return outputparam;
	}

	public boolean isSetInPreparedStatement() {
		return setInPreparedStatement;
	}

	void setSetInPreparedStatement() {
		this.setInPreparedStatement = true;
	}

	public String getMetaData() {
		return null;
	}	
	
	
	
}