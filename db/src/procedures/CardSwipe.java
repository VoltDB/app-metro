package procedures;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import org.voltdb.*;
import org.voltdb.types.TimestampType;

public class CardSwipe extends VoltProcedure {

    public final SQLStmt checkCard = new SQLStmt(
        "SELECT enabled, card_type, balance, expires, name, phone, email, notify FROM cards WHERE card_id = ?;");

    public final SQLStmt chargeCard = new SQLStmt(
        "UPDATE cards SET balance = ? WHERE card_id = ?;");
        
    public final SQLStmt checkStationFare = new SQLStmt(
        "SELECT fare, name FROM stations WHERE station_id = ?;");

    public final SQLStmt insertActivity = new SQLStmt(
        "INSERT INTO activity (card_id, date_time, station_id, activity_code, amount) VALUES (?,?,?,?,?);");

    public final SQLStmt exportActivity = new SQLStmt(
            "INSERT INTO card_alert_export (card_id, export_time, station_name, name, phone, email, notify, alert_message) VALUES (?,?,?,?,?,?,?,?);");
       
    private Random rand = new Random();
    
    // for returning results as a VoltTable
        final VoltTable resultTemplate = new VoltTable(
                new VoltTable.ColumnInfo("card_accepted",VoltType.TINYINT),
                new VoltTable.ColumnInfo("message",VoltType.STRING));
        
        public VoltTable buildResult(int accepted, String msg) {
                VoltTable r = resultTemplate.clone(64);
                r.addRow(accepted, msg);
                return r;
        }

        public static String intToCurrency(int i) {
                return String.format("%d.%02d", i/100, i%100);
        }
        
    public VoltTable run( int cardId,
                          int stationId
                          ) throws VoltAbortException {


        // check station fare, card status, get card owner's particulars
        voltQueueSQL(checkCard, EXPECT_ZERO_OR_ONE_ROW, cardId);
        voltQueueSQL(checkStationFare, EXPECT_ZERO_OR_ONE_ROW, stationId);
        VoltTable[] checks = voltExecuteSQL();
        VoltTable cardInfo = checks[0];
        VoltTable stationInfo = checks[1];

        // check that card exists
        if (cardInfo.getRowCount() == 0) {
                return buildResult(0,"Card Invalid");
        }

        // card exists, so advanceRow to read the record
        cardInfo.advanceRow();
        int enabled = (int)cardInfo.getLong(0);
        int cardType = (int)cardInfo.getLong(1);
        int balance = (int)cardInfo.getLong(2);
        TimestampType expires = cardInfo.getTimestampAsTimestamp(3);
        String owner = (String)cardInfo.getString(4);
        String phone = (String)cardInfo.getString(5);
        String email = (String)cardInfo.getString(6);
        int notify = (int)cardInfo.getLong(7);

        // read station fare
        stationInfo.advanceRow();
        int fare = (int)stationInfo.getLong(0);
        String stationName = (String)stationInfo.getString(1);

        int cardAccepted = 0;
        String message;
        int fareCharged = 0;
        
        // if card is disabled
        if (enabled == 0) { 
                return buildResult(0,"Card Disabled");
        }

        // check balance or expiration for valid cards
        if (cardType == 0) { // pay per ride
                if (balance > fare) {
                        // charge the fare
                        voltQueueSQL(chargeCard, balance-fare,cardId);
                        voltQueueSQL(insertActivity, cardId, getTransactionTime(), stationId, 1, fare);
                        voltExecuteSQL(true);
                        return buildResult(1,"Remaining Balance: "+intToCurrency(balance-fare));
                } else {
                        // insufficient balance
                        voltQueueSQL(insertActivity, cardId, getTransactionTime(), stationId, 0, 0);
                        if (rand.nextInt(10000) == 6) {
                            voltQueueSQL(exportActivity, cardId, getTransactionTime().getTime(), stationName, owner, phone, email, notify, "Insufficient Balance");
                        }
                        voltExecuteSQL(true);
                        return buildResult(0,"Card has insufficient balance: "+intToCurrency(balance));
                }
        } else { // unlimited card (e.g. monthly or weekly pass)
                if (expires.compareTo(new TimestampType(getTransactionTime())) > 0) {
                        voltQueueSQL(insertActivity, cardId, getTransactionTime(), stationId, 1, 0);
                        voltExecuteSQL(true);
                        return buildResult(1,"Card Expires: " + expires.toString());
                } else {
                        voltQueueSQL(insertActivity, cardId, getTransactionTime(), stationId, 0, 0);
                        voltExecuteSQL(true);
                        return buildResult(0,"Card Expired");
                }
        }
    }

}
