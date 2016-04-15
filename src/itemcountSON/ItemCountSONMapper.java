package itemcountSON;

import job.Optimizedjob;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static util.CombinationGenerator.combine;

public class ItemCountSONMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final IntWritable one = new IntWritable(1);
    private HashMap<String, Integer> hashMap = new HashMap<>();
    private HashSet<String> candidates = new HashSet<>();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Text word = new Text();
        String[] words = value.toString().split(" ");

        int n = Integer.parseInt(context.getConfiguration().get("n"));
        /*
         * Generate N-gram based on input parameter n
		 * 
		 */

        for (List<String> list : combine(words, words.length, n)) {
            String outputKey = list.toString();
            Integer count = hashMap.get(outputKey);
            if (candidates.contains(outputKey)) {
                continue;
            }

            if (count != null) {
                if (count >= 10) {
                    candidates.add(outputKey);
                    hashMap.remove(outputKey);
                    word.set(outputKey);
                    context.write(word, NullWritable.get());
                } else {
                    hashMap.put(outputKey, count + 1);
                }
            } else {
                hashMap.put(outputKey, 1);
            }
        }

    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        Optimizedjob.counter.incrementAndGet();
        while (context.nextKeyValue()) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        cleanup(context);
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        hashMap = new HashMap<>();
        candidates = new HashSet<>();
    }
}
