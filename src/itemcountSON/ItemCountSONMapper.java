package itemcountSON;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class ItemCountSONMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    private HashMap<Integer, Integer> hashMap = new HashMap<>(100000);
    private HashSet<Integer> candidates = new HashSet<>();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {


        String[] rawWords = value.toString().trim().split(" ");
        List<String> words = new ArrayList<>();

        for (String rawWord : rawWords) {
            if (rawWord.length() > 0) {
                words.add(rawWord);
            }
        }
        int[] ints = new int[words.size()];

        for (int i = 0; i < words.size(); i++) {

            ints[i] = Integer.parseInt(words.get(i));
        }
        int n = Integer.parseInt(context.getConfiguration().get("n"));
        /*
         * Generate N-gram based on input parameter n
		 * 
		 */
        combineHelper(context, ints, new int[n], 0, 0, words.size(), n);

    }

    private void combineHelper(Context context, int[] data, int[] list, int cur, int curLength, int n, int k)
            throws IOException, InterruptedException {
        if (curLength == k) {
            int outputKey = 0;
            int digit = 1;
            for (int j = list.length - 1; j >= 0; j--) {
                outputKey += digit * list[j];
                digit *= 1000;
            }

            if (candidates.contains(outputKey)) {
                return;
            }
            Integer count = hashMap.get(outputKey);

            if (count != null) {
                if (count >= 10) {
                    candidates.add(outputKey);
                    hashMap.remove(outputKey);
                    context.write(new IntWritable(outputKey), NullWritable.get());
                } else {
                    hashMap.put(outputKey, count + 1);
                }
            } else {
                hashMap.put(outputKey, 1);
            }
        } else {
            for (int i = cur; i < n; i++) {
                list[curLength] = data[i];
                combineHelper(context, data, list, i + 1, curLength + 1, n, k);
            }
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        while (context.nextKeyValue()) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
        }

        cleanup(context);

    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        hashMap = new HashMap<>(100000);
        candidates = new HashSet<>();
    }
}
