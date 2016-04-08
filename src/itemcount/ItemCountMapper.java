package itemcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    private void combineHelper(String[] data, List<List<String>> lists, List<String> list, int cur, int n, int k) {
        if (list.size() == k) {
            lists.add(new ArrayList<>(list));
        } else {
            for (int i = cur; i < n; i++) {
                list.add(data[i]);
                combineHelper(data, lists, list, i + 1, n, k);
                list.remove(list.size() - 1);
            }
        }
    }

    public List<List<String>> combine(String[] data, int n, int k) {
        List<List<String>> lists = new ArrayList<>();
        combineHelper(data, lists, new ArrayList<String>(), 0, n, k);

        return lists;
    }
}
