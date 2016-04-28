package aprioriMapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class AprioriPass1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> value,
                          Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable v : value){
            sum += v.get();
        }

        if(sum > Driver.MIN_SUPPORT) {
            context.write(key, NullWritable.get());
        }
    }
}
