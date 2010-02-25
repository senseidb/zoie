package proj.zoie.dataprovider.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public interface JDBCConnectionFactory {
	Connection getConnection() throws SQLException;
}
