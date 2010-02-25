package proj.zoie.dataprovider.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MysqlJDBCConnectionFactory implements JDBCConnectionFactory {
	private static final String MYSQL_JDBC_URL_PREFIX="jdbc:mysql://";
	private static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";
	
	private final String _username;
	private final String _pw;
	private final String _url;
	
	public MysqlJDBCConnectionFactory(String url,String username,String password){
		_url = MYSQL_JDBC_URL_PREFIX+url;
		_username = username;
		_pw = password;
	}
	public Connection getConnection() throws SQLException {
		try {
			Class.forName (MYSQL_DRIVER_NAME).newInstance ();
		} catch (Exception e) {
			throw new SQLException("unable to load driver: "+e.getMessage());
		}
        return DriverManager.getConnection (_url, _username, _pw);
	}
}
