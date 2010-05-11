package proj.zoie.dataprovider.jdbc.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import proj.zoie.dataprovider.jdbc.JDBCConnectionFactory;

public class OracleJDBCConnectionFactory implements JDBCConnectionFactory
{
  private static final String ORACLE_JDBC_URL_PREFIX="jdbc:oracle:thin:@";
  private static final String ORACLE_DRIVER_NAME = "oracle.jdbc.OracleDriver";

  private final String _username;
  private final String _password;
  private final String _url;
  

  private Connection _conn = null;

  public OracleJDBCConnectionFactory(String hostname, int port, String SID, String username, String password)
  {
    this(hostname +":" +port + ":" + SID, username, password);
  }

  public OracleJDBCConnectionFactory(String database, String username, String password)
  {
    _url = ORACLE_JDBC_URL_PREFIX + database;
    _username = username;
    _password = password;
  }

  public synchronized Connection getConnection() throws SQLException
  {
	if (_conn!=null){
	    try
	    {
	      //Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
	      Class.forName(ORACLE_DRIVER_NAME);
	    } catch (Exception e)
	    {
	      throw new SQLException("unable to load driver: "+e.getMessage());
	    }
        _conn = DriverManager.getConnection (_url, _username, _password);
	}
	return _conn;
  }
  
  public void showndown() throws SQLException{
		if (_conn!=null){
			_conn.close();
		}
	}
}
