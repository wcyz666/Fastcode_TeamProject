package itemcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

import static util.CombinationGenerator.combine;

public class ItemCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);

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
            word.set(list.toString());
            context.write(word, one);
        }
    }


}
