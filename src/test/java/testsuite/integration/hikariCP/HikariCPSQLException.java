package testsuite.integration.hikariCP;

import com.zaxxer.hikari.SQLExceptionOverride;

import java.sql.SQLException;

public class HikariCPSQLException implements SQLExceptionOverride {
    public Override adjudicate(final SQLException sqlException) {
        if (sqlException.getSQLState().equalsIgnoreCase("08S02") ||
            sqlException.getSQLState().equalsIgnoreCase("08007") ||
            sqlException.getSQLState().equalsIgnoreCase("08001")) {
            return Override.DO_NOT_EVICT;
        } else {
            return Override.CONTINUE_EVICT;
        }
    }
}
