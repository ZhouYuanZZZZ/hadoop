package com.zy.site;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class SiteRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream siteOutput = null;
    FSDataOutputStream otherOutput = null;

    public SiteRecordWriter(TaskAttemptContext job){
        // 获取文件系统
        FileSystem fs;
        try{
            fs = FileSystem.get(job.getConfiguration());

            // 创建输出文件路径
            Path pathSite = new Path("C:\\Users\\zy127\\Desktop\\hadoopFile\\site\\site.txt");
            Path pathOther = new Path("C:\\Users\\zy127\\Desktop\\hadoopFile\\site\\other.txt");

            // 创建输出流
            siteOutput = fs.create(pathSite);
            otherOutput = fs.create(pathOther);

        }catch (IOException e){
            e.printStackTrace();
        }
    }
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        if(key.toString().startsWith("http")){
            siteOutput.write((key.toString()+"\n").getBytes());
        }else{
            otherOutput.write((key.toString()+"\n").getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(siteOutput);
        IOUtils.closeStream(otherOutput);
    }
}
