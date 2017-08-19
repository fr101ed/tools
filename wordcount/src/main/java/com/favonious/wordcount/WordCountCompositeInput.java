package com.favonious.wordcount;

import com.hadoop.compression.lzo.LzoCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by liushiwei on 2017/8/11.
 */
public class WordCountCompositeInput extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCountCompositeInput(), args));

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        /* composite input setting */
        String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, args[0], args[1]);
        conf.set("mapreduce.join.expr", joinExpression);

        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(WordCountCompositeInput.class);
        job.setMapperClass(WordCountCompositeMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        /*
        * composite input setInputFormat
        * */
        job.setInputFormatClass(CompositeInputFormat.class);

        /* output compressor setting: output .lzo_deflate files */
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
