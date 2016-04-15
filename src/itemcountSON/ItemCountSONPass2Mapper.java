package itemcountSON;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.FileUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;

import static util.CombinationGenerator.combine;

public class ItemCountSONPass2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable ONE = new IntWritable(1);
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
            if (candidates.contains(outputKey)) {
                word.set(outputKey);
                context.write(word, ONE);
            }
        }

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String pass1File = context.getConfiguration().get("pass1File");
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(FileUtil.read(pass1File)));
        String candidate;
        while ((candidate = bufferedReader.readLine()) != null) {
            candidates.add(candidate.trim());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        candidates = new HashSet<>();
    }
}
