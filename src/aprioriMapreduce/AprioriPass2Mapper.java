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

public class AprioriPass2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable ONE = new IntWritable(1);
    private HashSet<String> candidates;

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

        for (int i = 0; i < words.length; i++) {
            if (candidates.contains(words[i])) {
                for (int j = i + 1; j < words.length; j++) {
                    if (candidates.contains(words[j])) {
                        word.set(words[i] + " " + words[j]);
                        context.write(word, ONE);
                    }
                }
            }

        }

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String pass1File = "tmp/pass1";
        candidates = new HashSet<>();
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(FileUtil.read(pass1File)));
        String candidate;
        while ((candidate = bufferedReader.readLine()) != null) {
            candidates.add(candidate.trim());
        }
    }

}
