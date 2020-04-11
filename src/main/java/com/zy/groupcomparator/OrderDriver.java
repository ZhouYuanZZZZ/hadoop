package com.zy.groupcomparator;

import com.zy.wccount.WcDriver;
import com.zy.wccount.WcMapper;
import com.zy.wccount.WcReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取Job实例
        Job job = Job.getInstance(new Configuration());

        //2.设置类路径
        job.setJarByClass(OrderDriver.class);

        //3.设置Mapper和Reducer
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //4. 设置输入输出类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //5. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\zy127\\Desktop\\hadoopFile\\orderin"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\zy127\\Desktop\\hadoopFile\\orderout"));

        job.setGroupingComparatorClass(OrderComparator.class);

        //6. 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
