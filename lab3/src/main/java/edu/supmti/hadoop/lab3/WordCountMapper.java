package edu.supmti.hadoop.lab3;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split("\\s+");

        for (String w : words) {
            if (!w.isEmpty()) {
                word.set(w);
                context.write(word, ONE);
            }
        }
    }
}
