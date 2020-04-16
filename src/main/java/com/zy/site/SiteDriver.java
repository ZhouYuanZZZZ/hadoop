package com.zy.site;

import com.zy.groupcomparator.OrderBean;
import com.zy.groupcomparator.OrderDriver;
import com.zy.groupcomparator.OrderMapper;
import com.zy.groupcomparator.OrderReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class SiteDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(SiteDriver.class);

        job.setMapperClass(SiteMapper.class);
        job.setReducerClass(SiteReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 将要自定义的输出格式组件设置到job中
        job.setOutputFormatClass(SiteOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\zy127\\Desktop\\hadoopFile\\site"));
        // 指定 FileOutputFormat 输出的_SUCCESS 文件输出目录
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\zy127\\Desktop\\hadoopFile\\site\\out"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
