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
	int[] balances = {5000,2000,1000,500,250,0};
	Calendar cal = Calendar.getInstance();
		
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

	    // generate cards
	    int card_id=0;
        System.out.println("Generating " + config.cardcount + " cards...");

        // check if cards already initialized
        int cardCount = client.callProcedure("@AdHoc","SELECT COUNT(*) FROM cards;").getResults()[0].getRowCount();
        if (cardCount > 0 && cardCount == config.cardcount)
	        return;

        for (int i=0; i<config.cardcount; i++) {

	        // default card (pay per fare)
	        int enabled = 1;		    
		    int card_type = 0;
		    int balance = balances[rand.nextInt(balances.length)];
		    TimestampType expires = null;

		    // disable 1/1000 cards
		    if (rand.nextInt(1000) == 0)
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
                                 i,
                                 enabled,
                                 card_type,
                                 balance,
                                 expires);

            if (i % 50000 == 0)
                System.out.println("  " + i);
            
        }
        System.out.println("  " + config.cardcount);
    }

    public void iterate() throws Exception {

        // Generate a card swipe
        int card_id = rand.nextInt(config.cardcount+1000); // use +1000 so sometimes we get an invalid card_id
        int station_id = stations.next();

        // call the 
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
