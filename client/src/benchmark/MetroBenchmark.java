package benchmark;

import java.util.*;
import org.voltdb.types.TimestampType;

public class MetroBenchmark extends BaseBenchmark {

    private Random rand = new Random();

    // constructor
    public MetroBenchmark(BenchmarkConfig config) {
        super(config);
        
    }

    public void initialize() throws Exception {

        System.out.println("Generating " + config.cardcount + " cards...");
        for (int i=0; i<config.cardcount; i++) {

            // randomly make 1/100 cards red, otherwise green
            String status = "green";
            if (rand.nextInt(100) == 0) 
                status = "red";

            client.callProcedure(new BenchmarkCallback("METROCARDS.insert"),
                                 "METROCARDS.insert",
                                 i,
                                 status);
            if (i % 50000 == 0)
                System.out.println("  " + i);
            
        }
        System.out.println("  " + config.cardcount);

    }

    public void iterate() throws Exception {

        // Generate a card swipe
        int card_id = rand.nextInt(config.cardcount+1); // use +1 so sometimes we get an invalid card
        TimestampType date_time = new TimestampType(System.currentTimeMillis());
        int location_id = rand.nextInt(500);

        // call the 
        client.callProcedure(new BenchmarkCallback("CardSwipe"),
                             "CardSwipe",
                             card_id,
                             date_time,
                             location_id
                             );
    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.getConfig("MetroBenchmark",args);
        
        BaseBenchmark benchmark = new MetroBenchmark(config);
        benchmark.runBenchmark();

    }
}
