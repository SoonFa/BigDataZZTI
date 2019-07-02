package test;/*
 *  create by Intellij IDEA
 *  @package name: test
 *  @author: 赵思绣
 *  @description:
 *  @date: 30 23:53
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

public class ComputerNumber {
    public static class StatMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable intValue = new IntWritable();
        private Text sexKey = new Text();
        private static Pattern pattern = Pattern.compile("\"(\\d+)\"\\s+\"+[\\u4e00-\\u9fa5]+\"\\s+\"(\\d)\"\\s+\"(\\d+)\"\\s+\"(\\d+)\"\\s+\"(\\d+)\"\\s+\"\\d+\"\\s+\"(\\d+)\"");
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            String sex="";
            String classid="";
            if (matcher.find()) {
                sex = matcher.group(2);
                classid = matcher.group(6);
            } else {
                return ;
            }
            try {
                if(classid.equals("2016080201"))
                {
                    if(sex.equals("1"))
                    {sexKey.set("计科161男");}
                    else
                    {sexKey.set("计科161女");}
                }
                else if (classid.equals("2016080202"))
                {
                    if(sex.equals("1"))
                    {sexKey.set("计科162男");}
                    else
                    {sexKey.set("计科162女");}
                }
                else if(classid.equals("2016080301"))
                {
                    if(sex.equals("1"))
                    {sexKey.set("网络161男");}
                    else
                    {sexKey.set("网络161女");}
                }
                else if(classid.equals("2016080302"))
                {
                    if(sex.equals("1"))
                    {sexKey.set("网络162男");}
                    else
                    {sexKey.set("网络162女");}
                }
                else if(classid.equals("2016080303"))
                {
                    if(sex.equals("1"))
                    {sexKey.set("网络163男");}
                    else
                    {sexKey.set("网络163女");}
                }
                else if(classid.equals("2016080401"))
                {
                    if(sex.equals("1"))
                    {sexKey.set("软件161男");}
                    else
                    {sexKey.set("软件161女");}
                }else if(classid.equals("2016080402"))
                {
                    if(sex.equals("1"))
                    {sexKey.set("软件162男");}
                    else
                    {sexKey.set("软件162女");}
                }
                else {
                    return ;
                }
                intValue.set(Integer.parseInt(sex));
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
        Job job = new Job(conf, "computernumber");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, args[0]);
        job.setJarByClass(ComputerNumber.class);
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


