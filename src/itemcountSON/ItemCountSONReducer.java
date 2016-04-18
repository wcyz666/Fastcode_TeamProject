package itemcountSON;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ItemCountSONReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> value,
                          Context context)
            throws IOException, InterruptedException {

        context.write(key, NullWritable.get());
    }
}
