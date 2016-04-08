package itemcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ItemCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> value,
                          Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable n : value)
            count += n.get();

        context.write(key, new IntWritable(count));
    }
}
