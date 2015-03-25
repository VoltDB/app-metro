package procedures;

import java.util.*;
import org.voltdb.*;
import org.voltdb.types.TimestampType;

public class GetSwipesPerSecond extends VoltProcedure {

    public final SQLStmt qry = new SQLStmt(
        "SELECT second, activities, entries "+
        "FROM secondly_stats "+
        "WHERE "+
        "  second >= ? AND "+
        "  second < TRUNCATE(SECOND, NOW) "+
        "ORDER BY second;");

    public VoltTable[] run(int seconds) throws VoltAbortException {

        // some seconds ago
        Date date = new Date(getTransactionTime().getTime() - (1000*seconds)); 
        
        voltQueueSQL(qry, date);
        return voltExecuteSQL(true);
        
    }

}
