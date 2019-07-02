package test;/*
 *  create by Intellij IDEA
 *  @package name: test
 *  @author: 赵思绣
 *  @description:
 *  @date: 30 19:38
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FiveCollege {
    public static class StatMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable intValue = new IntWritable();
        private Text sexKey = new Text();
        private static Pattern pattern = Pattern.compile("\"(\\d+)\"\\s+\"+[\\u4e00-\\u9fa5]+\"\\s+\"(\\d)\"\\s+\"(\\d+)\"\\s+\"(\\d+)\"\\s+\"(\\d+)\"\\s+\"\\d+\"\\s+\"(\\d+)\"");
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            String college="";
            String year="";
            if (matcher.find()) {
                college = matcher.group(3);
                year = matcher.group(5);
            } else {
                return ;
            }
            try {
                if(college.equals("000079"))
                {
                    if(year.equals("2014")) {sexKey.set("机电学院2014");}
                    else if (year.equals("2015")) {sexKey.set("机电学院2015");}
                    else if (year.equals("2016")) {sexKey.set("机电学院2016");}
                    else if (year.equals("2017")) {sexKey.set("机电学院2017");}
                    else if (year.equals("2018")) {sexKey.set("机电学院2018");}
                }
                else if(college.equals("000092"))
                {
                    if(year.equals("2014")) {sexKey.set("电信学院2014");}
                    else if(year.equals("2015")) {sexKey.set("电信学院2015");}
                    else if(year.equals("2016")) {sexKey.set("电信学院2016");}
                    else if(year.equals("2017")) {sexKey.set("电信学院2017");}
                    else if(year.equals("2018")) {sexKey.set("电信学院2018");}

                }
                else if(college.equals("000116"))
                {
                    if(year.equals("2014")) {sexKey.set("计算机学院2014");}
                    else if(year.equals("2015")) {sexKey.set("计算机学院2015");}
                    else if(year.equals("2016")) {sexKey.set("计算机学院2016");}
                    else if(year.equals("2017")) {sexKey.set("计算机学院2017");}
                    else if(year.equals("2018")) {sexKey.set("计算机学院2018");}
                }
                else if(college.equals("000139"))
                {
                    if(year.equals("2014")) {sexKey.set("服装学院2014");}
                    else if(year.equals("2015")) {sexKey.set("服装学院2015");}
                    else if(year.equals("2016")) {sexKey.set("服装学院2016");}
                    else if(year.equals("2017")) {sexKey.set("服装学院2017");}
                    else if(year.equals("2018")) {sexKey.set("服装学院2018");}
                }
                else if(college.equals("000157")) {
                    if (year.equals("2014")) { sexKey.set("经管学院2014"); }
                    else if (year.equals("2015")) { sexKey.set("经管学院2015"); }
                    else if (year.equals("2016")) { sexKey.set("经管学院2016"); }
                    else if (year.equals("2017")) { sexKey.set("经管学院2017"); }
                    else if (year.equals("2018")) { sexKey.set("经管学院2018"); }
                }
                else {
                    return ;
                }
                intValue.set(Integer.parseInt(year));
                context.write(sexKey, intValue);
            } catch (NumberFormatException e) {
                return ;
            }
        }
    }
    public static class StatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count++;
            }
            result.set(count);
            context.write(key, result);
        }
    }
    public static void main(String args[])
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "fivecollege");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, args[0]);
        job.setJarByClass(FiveCollege.class);
        job.setMapperClass(StatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setReducerClass(StatReducer.class);
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

