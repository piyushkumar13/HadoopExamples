package com.practice.hadoop.newapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NewWordCountJob extends Configured {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();
        /*
        This method sets the jar file in which each node will look for the Mapper and Reducer classes.
        It does not create a jar from the given class. Rather, it identifies the jar containing the given class.
        And yes, that jar file is "executed" (really the Mapper and Reducer in that jar file are executed) for
        the MapReduce job.

        Here you help Hadoop to find out that which jar it should send to nodes to perform Map and Reduce tasks.
        Your some-jar.jar might have various other jars in it's classpath, also your driver code might be in a
        separate jar than that of your Mapper and Reducer classes.

        Hence, using this setJarByClass method we tell Hadoop to find out the relevant jar by finding out that
        the class specified as it's parameter to be present as part of that jar. So usually you should provide
        either MapperImplementation.class or your Reducer implementation or any other class which is present in
        the same jar as that pf Mapper and Reducer. Also make sure that both Mapper and Reducer are part of the same jar.

        */
        job.setJarByClass(NewWordCountJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NewWordMapper.class);
        job.setReducerClass(NewWordReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
