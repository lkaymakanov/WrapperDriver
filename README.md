
  WrapperDriver - wrapper for PostgreSQL JDBC Driver. Implements all methods defined in JDBC API.
  Every method delegates execution to appropriate method in Postgre's JDBC driver. 



 
  WrapperDriver - a wrapper for PostgreSQL JDBC Driver.
  
  This driver allows:
   - Logging through a configurable driverLogger class.
   - Custom URL properties handling.
   - Debug mode to print connection info.
  
  JDBC URL example:
    wrapper:jdbc:postgresql://host:port/db?driverLogger=com.example.MyLogger
    wrapperdebug:jdbc:postgresql://host:port/db?driverLogger=com.example.MyLogger


Tomcat resource example:
<pre>
  Resource
      auth="Container"
      driverClassName="com.is.util.db.driver.wrapper.WrapperDriver"
      maxActive="30"
      maxIdle="10"
      maxWait="-1"
      name="jdbc/ltf"
      password="pass"
      type="javax.sql.DataSource"
      url="wrapper:jdbc:postgresql://host:port/db"
      username="user"
      stringtype="unspecified"
      testOnBorrow="true"
      validationQuery="SELECT 1"
      validationInterval="30000" 
</pre>

or in order to log sql queries to console.

<pre>
  Resource
      auth="Container"
      driverClassName="com.is.util.db.driver.wrapper.WrapperDriver"
      maxActive="30"
      maxIdle="10"
      maxWait="-1"
      name="jdbc/ltf"
      password="pass"
      type="javax.sql.DataSource"
      url="wrapperdebug:jdbc:postgresql://host:port/db"
      username="user"
      stringtype="unspecified"
      testOnBorrow="true"
      validationQuery="SELECT 1"
      validationInterval="30000" 
</pre>
 
 
