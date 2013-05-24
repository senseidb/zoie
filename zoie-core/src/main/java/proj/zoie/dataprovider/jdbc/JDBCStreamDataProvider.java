package proj.zoie.dataprovider.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

public class JDBCStreamDataProvider<T> extends StreamDataProvider<T> {
  private static final Logger log = Logger.getLogger(JDBCStreamDataProvider.class);
  private static final long DEFAULT_PULL_TIME = 1000;
  private final JDBCConnectionFactory _connFactory;
  private final PreparedStatementBuilder<T> _stmtBuilder;
  private String _version;
  private Connection _conn;
  private PreparedStatement _stmt;
  private ResultSet _res;
  private long _pullTime;

  public JDBCStreamDataProvider(JDBCConnectionFactory connFactory,
      PreparedStatementBuilder<T> stmtBuilder, Comparator<String> versionComparator) {
    super(versionComparator);
    _connFactory = connFactory;
    _stmtBuilder = stmtBuilder;
    _version = null;
    _conn = null;
    _stmt = null;
    _res = null;
    _pullTime = DEFAULT_PULL_TIME;
  }

  public void setPullTime(long pullTime) {
    _pullTime = pullTime;
  }

  public long getPullTime() {
    return _pullTime;
  }

  @Override
  public DataEvent<T> next() {
    DataEvent<T> event = null;
    try {
      if (!_res.next()) {
        try {
          _res.close();
        } finally {
          _stmt.close();
        }
        try {
          Thread.sleep(_pullTime);
        } catch (InterruptedException e) {
          log.error(e.getMessage(), e);
        }
        _stmt = _stmtBuilder.buildStatment(_conn, _version);
        _res = _stmt.executeQuery();
      } else {
        event = _stmtBuilder.buildDataEvent(_res);
        _version = event.getVersion();
      }
    } catch (SQLException sqle) {
      log.error(sqle.getMessage(), sqle);
    }
    return event;
  }

  @Override
  public void setStartingOffset(String version) {
    _version = version;
  }

  @Override
  public void reset() {
    if (_res != null) {
      try {
        _res.close();
      } catch (SQLException sqle) {
        log.error(sqle.getMessage(), sqle);
        _res = null;
      } finally {
        try {
          _stmt.close();
        } catch (SQLException e) {
          log.error(e.getMessage(), e);
        }
      }
    }

    DataConsumer<T> dc = getDataConsumer();
    if (dc == null) {
      // ? Hao: needs to fix later
      _version = null;
      log.warn("problem opening index, maynot exist, defaulting version to null");
      // log.warn("problem opening index, maynot exist, defaulting version to 0");
    } else {
      _version = dc.getVersion();
    }
    if (_conn == null) {
      try {
        _conn = _connFactory.getConnection();
        _stmt = _stmtBuilder.buildStatment(_conn, _version);
        _res = _stmt.executeQuery();
      } catch (SQLException sqle) {
        log.fatal(sqle.getMessage(), sqle);
        _res = null;
      }
    }
  }

  @Override
  public void stop() {
    try {
      super.stop();
    } finally {
      try {
        if (_res != null) {
          _res.close();
        }
      } catch (SQLException sqle) {
        log.error(sqle.getMessage(), sqle);
      } finally {
        try {
          if (_stmt != null) {
            _stmt.close();
          }
        } catch (SQLException sqle) {
          log.error(sqle.getMessage(), sqle);
        }
      }
    }
  }
}
