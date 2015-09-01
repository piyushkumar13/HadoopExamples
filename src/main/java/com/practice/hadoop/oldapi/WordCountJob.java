package com.practice.hadoop.oldapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountJob extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {

        if (strings.length < 2) {
            System.out.println("Please provide the input and output property");
        }

        JobConf jobConf = new JobConf(WordCountJob.class);
        jobConf.setJobName("Word Count Job");

        FileInputFormat.setInputPaths(jobConf, new Path(strings[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path((strings[1])));

        jobConf.setMapperClass(WordMapper.class);
        jobConf.setReducerClass(WordReducer.class);

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(IntWritable.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(jobConf);

        return 0;
    }

    @Override
    public void setConf(Configuration entries) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountJob(), args);
        System.exit(exitCode);
    }
}
