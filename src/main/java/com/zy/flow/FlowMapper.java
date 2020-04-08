package com.zy.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");

        phone.set(line[1]);

        long upFlow = Long.parseLong(line[line.length - 3]);
        long downFlow = Long.parseLong(line[line.length - 2]);

        flow.setUpFlow(upFlow);
        flow.setDownFlow(downFlow);
        flow.setSumFlow(upFlow+downFlow);

        context.write(phone,flow);

    }
}
