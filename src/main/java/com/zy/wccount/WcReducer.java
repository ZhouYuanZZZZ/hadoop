package com.zy.wccount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WcReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

    private long sum = 0;
    private LongWritable v = new LongWritable();


    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        for (LongWritable count:values) {
            sum += count.get();
        }

        v.set(sum);
        context.write(key,v);
    }
}
