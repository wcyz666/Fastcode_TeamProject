package aprioriMapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class AprioriPass3Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> value,
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
