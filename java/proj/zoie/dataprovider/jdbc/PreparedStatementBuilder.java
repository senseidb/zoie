package proj.zoie.dataprovider.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import proj.zoie.api.DataConsumer.DataEvent;

public interface PreparedStatementBuilder<T> {
	PreparedStatement buildStatment(Connection conn, long fromVersion) throws SQLException;
	
	/**
	 * <b>The builder should not ever change the cursor of the result set. It should only work on the current row.</b>
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	DataEvent<T> buildDataEvent(ResultSet rs) throws SQLException;
}
