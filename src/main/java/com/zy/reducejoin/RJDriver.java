package com.zy.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RJDriver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(RJDriver.class);

        job.setMapperClass(RJMapper.class);
        job.setReducerClass(RJReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(RJComparator.class);

        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\zy127\\Desktop\\hadoopFile\\rj\\in"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\zy127\\Desktop\\hadoopFile\\rj\\out"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
