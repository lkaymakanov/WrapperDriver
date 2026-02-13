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
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * <pre>
 * WrapperDriver - a wrapper for PostgreSQL JDBC Driver.
 * 
 * This driver allows:
 *  - Logging through a configurable driverLogger class.
 *  - Custom URL properties handling.
 *  - Debug mode to print connection info.
 * 
 * JDBC URL example:
 *   wrapper:jdbc:postgresql://host:port/db?driverLogger=com.example.MyLogger
 * 
 * <Resource
 *     auth="Container"
 *     driverClassName="com.is.util.db.driver.wrapper.WrapperDriver"
 *     maxActive="30"
 *     maxIdle="10"
 *     maxWait="-1"
 *     name="jdbc/ltf"
 *     password="12345"
 *     type="javax.sql.DataSource"
 *     url="wrapper:jdbc:postgresql://host:port/db?driverLogger=com.example.MyLogger"
 *     username="staging"
 *     stringtype="unspecified"
 *     testOnBorrow="true"
 *     validationQuery="SELECT 1"
 *     validationInterval="30000"
/>
 * </pre>
 */

public class WrapperDriver implements Driver {

    // JDBC URL prefixes
    protected static final String JDBC_URL_PREFIX = "wrapper:";
    protected static final String JDBC_DEBUG_URL_PREFIX = "wrapperdebug:";

    // Driver version info
    protected static final int DRIVER_MAJOR_VERSION = 0;
    protected static final int DRIVER_MINOR_VERSION = 3;
    protected static final boolean DRIVER_JDBC_COMPIANT = true;

    // Property name for wrapped driver class (not currently used, kept for compatibility)
    protected static final String WRAPPED_DRIVER_CLASS_PEOPERTY = "WRAPPED_DRIVER";

    // Global registry of driver loggers by class (weak references for container safety)
    static final Map<Class<?>, IDriverLogger> LOGGERS = Collections.synchronizedMap(new WeakHashMap<>());
    
    //register test logger
    static {
    	LOGGERS.putIfAbsent(SysoLogger.class, new SysoLogger());
    }
    
    static final Consumer<String> logFunction = WrapperDriver::log;

    // Register the wrapper driver with DriverManager at class load time
    static {
        try {
            java.sql.DriverManager.registerDriver(new WrapperDriver());
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    public WrapperDriver() {
    }

    /**
     * Connect to the database.
     * 
     * Steps:
     *  1. Determine debug mode based on URL prefix.
     *  2. Clean wrapper prefix from URL.
     *  3. Extract custom properties from URL and merge into connection Properties.
     *  4. Remove custom properties from URL for real driver.
     *  5. Load the real PostgreSQL driver and get the connection.
     *  6. Register logger if "driverLogger" property is set.
     *  7. Wrap the connection in WrapperConnection.
     */
    public Connection connect(String url, java.util.Properties info) throws SQLException {
        try {
            boolean debugMode = url.startsWith(JDBC_DEBUG_URL_PREFIX);

            // Remove wrapper prefixes from URL
            String cleanUrl = url.replace(JDBC_URL_PREFIX, "").replace(JDBC_DEBUG_URL_PREFIX, "");

            // Extract and merge custom properties from URL into connection info
            java.util.Properties customProps = extractCustomProps(cleanUrl);
            for (String key : customProps.stringPropertyNames()) {
                info.setProperty(key, customProps.getProperty(key));
            }

            // Remove custom props from URL for the real driver
            cleanUrl = removeCustomPropsFromUrl(cleanUrl, customProps);
            
            // Register logger if configured
            registerLoggerIfNeeded(info);

            // Load the real PostgreSQL driver
            Class.forName("org.postgresql.Driver");
            Driver driver = DriverManager.getDriver(cleanUrl);

            // Print debug information
            sysout(url, cleanUrl, debugMode, info);

            // Create the actual connection and wrap it
            Connection mc = driver.connect(cleanUrl, info);
            WrapperConnection conn = new WrapperConnection(mc, null, debugMode);

            return conn;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "You must set wrapped driver class name in connection property "
                            + WRAPPED_DRIVER_CLASS_PEOPERTY + "!",
                    e);
        }
    }

    // --------------------------- Helper Methods ---------------------------

    /**
     * Registers a logger if the "driverLogger" property is set in the connection properties.
     * The property is removed after reading to avoid passing it to the real driver.
     * Only one instance of each logger class is created.
     */
    private static void registerLoggerIfNeeded(Properties info) {
        String loggerClassName = info.getProperty("driverLogger");

        if (loggerClassName == null || loggerClassName.isEmpty()) {
            return;
        }

        // Remove the property to avoid leaking to PostgreSQL driver
        info.remove("driverLogger");

        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class<?> clazz = Class.forName(loggerClassName, true, cl);

            if (!IDriverLogger.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(loggerClassName + " does not implement IDriverLogger");
            }

            synchronized (LOGGERS) {
                if (!LOGGERS.containsKey(clazz)) {
                    IDriverLogger logger = (IDriverLogger) clazz.getDeclaredConstructor().newInstance();
                    LOGGERS.put(clazz, logger);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize driver logger: " + loggerClassName, e);
        }
    }

    /**
     * Extracts custom properties from the query string of the URL (after '?').
     * Returns an empty Properties object if no custom properties exist.
     */
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

    /**
     * Logs a message to all registered driver loggers.
     */
    private static void log(String s) {
        synchronized (LOGGERS) {
            for (IDriverLogger l : LOGGERS.values()) {
                l.log(s);
            }
        }
    }

    /**
     * Prints connection info to stdout for debugging.
     * Masks password properties for security.
     */
    private static void sysout(String originalUrl, String cleanUrl, boolean debugMode, Properties info) {
        logFunction.accept("=== WrapperDriver CONNECT ===");
        logFunction.accept("Original URL: " + originalUrl);
        logFunction.accept("Clean URL: " + cleanUrl);
        logFunction.accept("Debug mode: " + debugMode);

        if (info != null && !info.isEmpty()) {
        	logFunction.accept("Properties:");
            info.forEach((k, v) -> {
                String key = String.valueOf(k);
                String value = "password".equalsIgnoreCase(key) ? "****" : String.valueOf(v);
                log("  " + key + " = " + value);
            });
        }
        logFunction.accept("================================");
    }

    /**
     * Removes the custom properties (parsed from URL) from the URL string,
     * so the underlying PostgreSQL driver receives a clean URL.
     */
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
        return idx < 0 ? url : url.substring(0, idx); // returns base URL without query
    }

    /**
     * Determines if the driver accepts the given URL.
     * Accepts URLs with "wrapper:" or "wrapperdebug:" prefix.
     */
    public boolean acceptsURL(String url) throws SQLException {
        String s = url.toString();
        return (s.indexOf(JDBC_URL_PREFIX) >= 0 || s.indexOf(JDBC_DEBUG_URL_PREFIX) >= 0);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info) throws SQLException {
        // Not used; return empty array
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
        // Not implemented
        return null;
    }
}
