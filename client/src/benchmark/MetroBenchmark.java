package benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.voltdb.types.TimestampType;

public class MetroBenchmark extends BaseBenchmark {

    private Random rand = new Random();

    // for random data generation
    private RandomCollection<Integer> stations = new RandomCollection<Integer>();
    int[] balances = {5000,2000,1000,500};
    Calendar cal = Calendar.getInstance();
    int cardCount = 0;
    int max_station_id = 0;
		
    // constructor
    public MetroBenchmark(BenchmarkConfig config) {
        super(config);

        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

    }


    public void initialize() throws Exception {

        // load stations
        FileReader fileReader = new FileReader(config.stationfilename);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        CsvLineParser parser = new CsvLineParser();
        Iterator it;
        int station_id = 0;
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            it = parser.parse(line);

            String name = (String)it.next();
            int weight = Integer.parseInt((String)it.next());
            int fare = 250;

            stations.add(weight,station_id);
            client.callProcedure(new BenchmarkCallback("STATIONS.upsert"),
                                 "STATIONS.upsert",
                                 station_id++,
                                 name,
                                 fare);
        }
        bufferedReader.close();
        max_station_id = station_id;

        // generate cards
        int card_id=0;
        System.out.println("Generating " + config.cardcount + " cards...");

        // check if cards already initialized
        cardCount = client.callProcedure("@AdHoc","SELECT COUNT(*) FROM cards;").getResults()[0].getRowCount();

        for (int i=cardCount; i<config.cardcount; i++) {
            generateCard();
            if (i+1 % 5000 == 0)
                System.out.println("  " + i);
        }
    }

    public int randomizeNotify() throws Exception {
        // don't notify 90% (0), 5% text (1), 5% email (2)
        float n = rand.nextFloat();
        if (n > 0.1) {
            return(0);
        }
        if (n > 0.05) {
            return(1);
        }
        return(2);
    }

    public void generateCard() throws Exception {

        // default card (pay per fare)
        int enabled = 1;		    
        int card_type = 0;
        int balance = balances[rand.nextInt(balances.length)];
        String preName = "T Ryder ";
        String phone = "6174567890";
        String email = "tryder@gmail.com";
        int notify = randomizeNotify();
        TimestampType expires = null;

        // disable 1/10000 cards
        if (rand.nextInt(10000) == 0)
            enabled = 0;

        // make 1/3 cards unlimited (weekly or monthly pass)
        if (rand.nextInt(3) == 0) {
            card_type = 1;
            balance = 0;
            // expired last night at midnight, or any of the next 30 days
            Calendar cal2 = (Calendar)cal.clone();
            cal2.add(Calendar.DATE,rand.nextInt(30));
            expires = new TimestampType(cal2.getTime());
        }

        client.callProcedure(new BenchmarkCallback("CARDS.upsert"),
                             "CARDS.upsert",
                             ++cardCount,
                             enabled,
                             card_type,
                             balance,
                             expires,
                             preName + cardCount, // create synthetic numeric person
                             phone,
                             email,
                             notify);
		
    }
	
    public void iterate() throws Exception {

        // sometimes create a new card
        if (rand.nextInt(25) == 0)
            generateCard();

        // sometimes replenish a card
        if (rand.nextInt(5) == 0) {
            client.callProcedure(new BenchmarkCallback("ReplenishCard"),
                                 "ReplenishCard",
                                 balances[rand.nextInt(balances.length)],
                                 rand.nextInt(cardCount)
                                 );
        }

        // card swipe
        int card_id = rand.nextInt(cardCount+1000); // use +1000 so sometimes we get an invalid card_id

        int station_id = 0; 
        if (rand.nextInt(5) == 0) {
            station_id = rand.nextInt(max_station_id); // sometimes pick a random station
        } else {
            station_id = stations.next(); // pick a station based on the weights
        }

        client.callProcedure(new BenchmarkCallback("CardSwipe"),
                             "CardSwipe",
                             card_id,
                             station_id
                             );
    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.getConfig("MetroBenchmark",args);
        
        BaseBenchmark benchmark = new MetroBenchmark(config);
        benchmark.runBenchmark();

    }
}
