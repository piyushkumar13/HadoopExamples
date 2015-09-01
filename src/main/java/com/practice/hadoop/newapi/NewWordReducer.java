package com.practice.hadoop.newapi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NewWordReducer extends Reducer {

    public void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        int count = 0;

        while (values.iterator().hasNext()) {
            count += values.iterator().next().get();
        }
        context.write(key, new IntWritable(count));
    }
}
