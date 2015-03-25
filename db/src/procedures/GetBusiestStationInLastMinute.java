package procedures;

import java.util.*;
import org.voltdb.*;
import org.voltdb.types.TimestampType;

public class GetBusiestStationInLastMinute extends VoltProcedure {

    public final SQLStmt qry = new SQLStmt(
        "SELECT s.name, SUM(v.activities) as swipes, SUM(v.entries) as entries "+
        "FROM secondly_entries_by_station v "+
        "INNER JOIN stations s ON s.station_id = v.station_id "+
        "WHERE "+
        "  v.second >= ? "+
        "GROUP BY s.name "+
        "ORDER BY swipes DESC;");

    public VoltTable[] run() throws VoltAbortException {

        // 6 seconds ago
        Date date = new Date(getTransactionTime().getTime() - 60000); 
        
        voltQueueSQL(qry, date);
        return voltExecuteSQL(true);
        
    }

}
