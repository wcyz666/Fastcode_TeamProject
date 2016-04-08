package itemcount;

import job.Optimizedjob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import util.SimpleParser;

import java.io.IOException;

public class Driver {

    public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);

        String input = parser.get("input");
        String output = parser.get("output");
        String N = parser.get("n");

        getJobFeatureVector(input, output, N);

    }

    private static void getJobFeatureVector(String input, String output, String N)
            throws IOException, ClassNotFoundException, InterruptedException {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Compute NGram Count");

        job.setClasses(ItemCountMapper.class, ItemCountReducer.class, ItemCountReducer.class);
        job.setMapOutputClasses(Text.class, IntWritable.class);
        job.setParameter("n", N);
        job.run();
    }
}
