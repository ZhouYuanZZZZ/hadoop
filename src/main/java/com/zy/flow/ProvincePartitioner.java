package com.zy.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {

        // 分区逻辑 返回分区编号 从0开始
        String preNum = text.toString().substring(0, 3);

        if ("136".equals(preNum)) {
            return 0;
        }
        if ("137".equals(preNum)) {
            return 1;
        }

        if ("138".equals(preNum)) {
            return 2;
        }

        if ("139".equals(preNum)) {
            return 3;
        }

        return 4;
    }
}
