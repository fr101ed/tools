package com.favonious.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liushiwei on 2017/8/19.
 */
public class WordCountMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.toString()), value);
    }
}