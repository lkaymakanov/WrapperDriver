package com.is.util.db.driver.wrapper;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.logging.Logger;

/**
 * 
 * DigestDriver - wrapper for PostgreSQL JDBC Driver. Implements all methods defined in JDBC API.
 * Every method converts input and output parameters, if necessary, to make them PostgreSQL compatible, then delegates 
 * execution to appropriate method in Postgre's JDBC driver. 
 * 
 * 
 *     <Resource
			auth="Container"
			driverClassName="com.is.util.db.driver.wrapper.WrapperDriver"
			maxActive="30"
			maxIdle="10"
			maxWait="-1"
			name="jdbc/ltf"
			password="12345"
			type="javax.sql.DataSource"
			url="wrapper:jdbc:postgresql://10.240.44.244:5434/sofiamerge3?tetsHandler=com.test.TestHndler"
			username="staging"
			stringtype="unspecified"
			testOnBorrow="true"
			validationQuery="SELECT 1"
			validationInterval="30000"
			/>
 *
 */
public class WrapperDriver implements Driver {

	//private static final Category LOG = Category.getInstance(ReportInformixDriver.class);
	protected static final String 	JDBC_URL_PREFIX		= "wrapper:";
	protected static final String 	JDBC_DEBUG_URL_PREFIX= "wrapperdebug:";
	protected static final int 		DRIVER_MAJOR_VERSION= 0;
	protected static final int 		DRIVER_MINOR_VERSION= 3;
	protected static final boolean	DRIVER_JDBC_COMPIANT= true;
	protected static final String 	WRAPPED_DRIVER_CLASS_PEOPERTY= "WRAPPED_DRIVER";
	static final Map<Class<?>, IDriverLogger> LOGGERS = Collections.synchronizedMap(new WeakHashMap<>());
	
	static {
		try {
			java.sql.DriverManager.registerDriver(new WrapperDriver());
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
	}

	public WrapperDriver(){
	}
	
	
	
	
	public Connection connect(String url, java.util.Properties info) throws SQLException {
		
		try {
			
			boolean debugMode = url.startsWith(JDBC_DEBUG_URL_PREFIX);

	        // Clean the wrapper prefix
	        String cleanUrl = url.replace(JDBC_URL_PREFIX, "").replace(JDBC_DEBUG_URL_PREFIX, "");

	        // Parse custom properties from URL
	        java.util.Properties customProps = extractCustomProps(cleanUrl);
	        for (String key : customProps.stringPropertyNames()) {
	            info.setProperty(key, customProps.getProperty(key));
	        }

	        // Remove custom props from the URL for the real driver
	        cleanUrl = removeCustomPropsFromUrl(cleanUrl, customProps);

	        // Load the real driver
	        Class.forName("org.postgresql.Driver");
	        Driver driver = DriverManager.getDriver(cleanUrl);
			sysout(url, cleanUrl, debugMode, info);
			
			//register logger 
			registerLoggerIfNeeded(info);
			
			//Connection testConnection = null; // debugMode? driver.connect(url, info) : null;
			Connection mc = driver.connect(cleanUrl, info);
			WrapperConnection conn = new WrapperConnection(mc, null, debugMode);
			
			return conn;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("You must set wrapped driver class name in connection property "+WRAPPED_DRIVER_CLASS_PEOPERTY+"!",e);
		}
	}
	
	
	
	// --------------------------- Helper Methods ---------------------------
	
	private static void registerLoggerIfNeeded(Properties info) {
	    String loggerClassName = info.getProperty("driverLogger");
	   
	    if (loggerClassName == null || loggerClassName.isEmpty()) {
	        return;
	    }
	    info.remove("driverLogger");

	    try {
	    	ClassLoader cl = Thread.currentThread().getContextClassLoader();
	        Class<?> clazz = Class.forName(loggerClassName, true, cl);

	        if (!IDriverLogger.class.isAssignableFrom(clazz)) {
	            throw new IllegalArgumentException(
	                loggerClassName + " does not implement IDriverLogger");
	        }

	        synchronized (LOGGERS) {
	            if (!LOGGERS.containsKey(clazz)) {
	                IDriverLogger logger =
	                        (IDriverLogger) clazz.getDeclaredConstructor().newInstance();
	                LOGGERS.put(clazz, logger);
	            }
	        }

	    } catch (Exception e) {
	        throw new RuntimeException(
	                "Failed to initialize driver logger: " + loggerClassName, e);
	    }
	}

	private Properties extractCustomProps(String url) {
	    Properties props = new Properties();
	    int idx = url.indexOf('?');
	    if (idx < 0) return props;

	    String query = url.substring(idx + 1);
	    for (String pair : query.split("&")) {
	        if (pair.contains("=")) {
	            String[] parts = pair.split("=", 2);
	            props.setProperty(parts[0], parts[1]);
	        }
	    }
	    return props;
	}
	
	static void log(String s) {
	    synchronized (LOGGERS) {
	        for (IDriverLogger l : LOGGERS.values()) {
	            l.log(s);
	        }
	    }
	}
	
	private static void sysout(String originalUrl,
            String cleanUrl,
            boolean debugMode,
            java.util.Properties info) {

		System.out.println("=== WrapperDriver CONNECT ===");
		
		System.out.println("Original URL: " + originalUrl);
		System.out.println("Clean URL: " + cleanUrl);
		System.out.println("Debug mode: " + debugMode);
		
		if (info != null && !info.isEmpty()) {
		System.out.println("Properties:");
		info.forEach((k, v) -> {
		String key = String.valueOf(k);
		String value = "password".equalsIgnoreCase(key)
		     ? "****"
		     : String.valueOf(v);
		
		System.out.println("  " + key + " = " + value);
		});
		}
		
		System.out.println("================================");
	}


	private String removeCustomPropsFromUrl(String url, Properties customProps) {
	    if (customProps.isEmpty()) return url;
	    int idx = url.indexOf('?');
	    if (idx < 0) return url;

	    StringBuilder sb = new StringBuilder();
	    sb.append(url, 0, idx).append('?');

	    String query = url.substring(idx + 1);
	    String[] pairs = query.split("&");
	    boolean first = true;
	    for (String pair : pairs) {
	        String key = pair.contains("=") ? pair.split("=", 2)[0] : pair;
	        if (!customProps.containsKey(key)) {
	            if (!first) sb.append('&');
	            sb.append(pair);
	            first = false;
	        }
	    }
	    return  idx < 0 ? url : url.substring(0, idx) ;//  sb.toString();
	}
	
	

	public boolean acceptsURL(String url) throws SQLException {
		String s = url.toString();
		return (s.indexOf(JDBC_URL_PREFIX)>=0 || s.indexOf(JDBC_DEBUG_URL_PREFIX)>=0);
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info) throws SQLException {
		return new DriverPropertyInfo[]{};
	}

	public int getMajorVersion() {
		return DRIVER_MAJOR_VERSION;	
	}

	public int getMinorVersion() {
		return DRIVER_MINOR_VERSION;
	}


	public boolean jdbcCompliant() {
		return DRIVER_JDBC_COMPIANT;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}
	
}
