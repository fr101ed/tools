package com.favonious.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;

/**
 * Created by liushiwei on 2017/8/19.
 */
public class WordCountCompositeMapper extends Mapper<Text, TupleWritable, Text, Text> {
    @Override
    public void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
        for (Writable writable : value) {
            context.write(key, new Text(writable.toString()));
            return;
        }
    }
}
