package edu.supmti.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString());
            String[] tokens = value.toString().split("\\s+");
            for (String token : tokens) {
                if (!token.isEmpty()) {
                    word.set(token);
                    context.write(word, one);
                }
            }
    }
}
