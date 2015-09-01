package com.practice.hadoop.oldapi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WordReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    public void reduce(Text text, Iterator<IntWritable> intWritableIterator, OutputCollector<Text, IntWritable> textIntWritableOutputCollector, Reporter reporter) throws IOException {

        int count = 0;

        while (intWritableIterator.hasNext()){
            count+= intWritableIterator.next().get();
        }
        textIntWritableOutputCollector.collect(text,new IntWritable(count));
    }
}
