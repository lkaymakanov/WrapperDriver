package com.is.util.db.driver.wrapper;

class SysoLogger implements IDriverLogger{

	@Override
	public String log(String s) {
		System.out.println(s);
		return s;
	}

}
