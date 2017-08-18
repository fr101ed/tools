package com.favonious.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by liushiwei on 2017/8/11.
 */
public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCount(), args));

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
//        String[] arr = new String[2];
//        arr[0] = args[0];
//        arr[0] = args[1];
//        String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, arr);
//        conf.set("mapreduce.join.expr", joinExpression);


//        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setCompressOutput(job, true);
//        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class TokenizerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class IntSumReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private JSONObject output = new JSONObject();

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                JSONObject valueJson = new JSONObject(value.toString());
                for (String cate : valueJson.keySet()) {
                    if (output.has(cate)) {
                        output.put(cate, output.getInt(cate) + valueJson.getInt(cate));
                    } else {
                        output.put(cate, valueJson.getInt(cate));
                    }
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(1), new Text(output.toString()));
        }
    }
}
