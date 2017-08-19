package com.favonious.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by liushiwei on 2017/8/19.
 */
public class WordCountReducer extends Reducer<Text, Text, Text, Text> {
    private JSONObject output = new JSONObject();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        JSONArray valueList = new JSONArray();
        for (Text value : values) {
            valueList.put(value.toString());
        }
        output.put(key.toString(), valueList);
//        context.write(key, new Text(output.toString()));
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("end"), new Text(output.toString()));
    }
}