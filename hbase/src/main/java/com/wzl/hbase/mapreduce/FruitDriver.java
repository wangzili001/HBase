package com.wzl.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FruitDriver extends Configuration implements Tool {
    private Configuration conf = null;

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        FruitDriver fruitDriver = new FruitDriver();
        ToolRunner.run(configuration,fruitDriver,args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf);

        job.setJarByClass(FruitDriver.class);

        //指定Mapper
        TableMapReduceUtil.initTableMapperJob("fruit",
                new Scan(),
                FruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);
        TableMapReduceUtil.initTableReducerJob("fruit_mr",
                FruitReducer.class,
                job);
        return job.waitForCompletion(true)?1:0;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
