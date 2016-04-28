package aprioriMapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class AprioriPass1Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {


        String[] rawWords = value.toString().trim().split(" ");

        for (String rawWord : rawWords) {
            if (rawWord.length() > 0) {
                context.write( new IntWritable(Integer.valueOf(rawWord)),new IntWritable(1));
            }
        }


    }




}
