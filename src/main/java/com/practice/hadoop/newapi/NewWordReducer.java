package com.practice.hadoop.newapi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class NewWordReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws IOException, InterruptedException {
        int count = 0;

        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            count += iterator.next().get();
        }
        context.write(key, new IntWritable(count));
    }
}
