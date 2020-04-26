package com.zy.topnflow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowBean flowBean = (FlowBean) o;
        return upFlow == flowBean.upFlow &&
                downFlow == flowBean.downFlow &&
                sumFlow == flowBean.sumFlow;
    }

    @Override
    public int hashCode() {
        return Objects.hash(upFlow, downFlow, sumFlow);
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        int result = 0;
        if(this.sumFlow > o.getSumFlow()){
            result = -1;
        }
        if(this.sumFlow == o.getSumFlow()){
            result = 0;
        }
        if(this.sumFlow < o.getSumFlow()){
            result = 1;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }
}
