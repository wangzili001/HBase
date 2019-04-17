package com.wzl.hbase.mapreduece2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSDriver extends Configuration implements Tool {
    private Configuration configuration;

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        System.exit(ToolRunner.run(configuration, new HDFSDriver(), args));
    }
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);

        job.setJarByClass(HDFSDriver.class);

        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob("fruit",
                HDFSReducer.class,
                job);
        //设置输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        return job.waitForCompletion(true)?1:0;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}
