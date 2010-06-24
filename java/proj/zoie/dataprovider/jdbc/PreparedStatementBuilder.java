package proj.zoie.dataprovider.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.ZoieVersion;

public interface PreparedStatementBuilder<T, V extends ZoieVersion> {
	PreparedStatement buildStatment(Connection conn, V fromVersion) throws SQLException;
	
	/**
	 * <b>The builder should not ever change the cursor of the result set. It should only work on the current row.</b>
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	DataEvent<T,V> buildDataEvent(ResultSet rs) throws SQLException;
}
