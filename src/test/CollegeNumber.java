package test;/*
 *  create by Intellij IDEA
 *  @package name: test
 *  @author: 赵思绣
 *  @description:
 *  @date: 30 11:20
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

public class CollegeNumber {
    public static class StatMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable intValue = new IntWritable();
        private Text sexKey = new Text();
        private static Pattern pattern = Pattern.compile("\"(\\d+)\"\\s+\"+[\\u4e00-\\u9fa5]+\"\\s+\"(\\d)\"\\s+\"(\\d+)\"\\s+\"(\\d+)\"\\s+\"\\d+\"\\s+\"\\d+\"\\s+\"(\\d+)\"");
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            String college="";
            if (matcher.find()) {
                college = matcher.group(3);
            } else {
                return ;
            }
            try {
                if(college.equals("000058")) { sexKey.set("纺织学院");}
                 else if (college.equals("000069")){ sexKey.set("能环学院"); }
                 else if(college.equals("000079")){sexKey.set("机电学院");}
                 else if(college.equals("000092")){sexKey.set("电信学院");}
                 else if(college.equals("000106")){sexKey.set("材化学院");}
                 else if(college.equals("000116")){sexKey.set("计算机学院");}
                 else if(college.equals("000126")){sexKey.set("建工学院");}
                 else if(college.equals("000139")){sexKey.set("服装学院");}
                 else if(college.equals("000146")){sexKey.set("艺设学院");}
                 else if(college.equals("000157")){sexKey.set("经管学院");}
                 else if(college.equals("000172")){sexKey.set("新传学院");}
                 else if(college.equals("000198")){sexKey.set("外国语学院");}
                 else if(college.equals("000209")){sexKey.set("理学院");}
                 else if(college.equals("000221")){sexKey.set("职业学院");}
                 else if(college.equals("000274")){sexKey.set("亚太国际学院");}
                 else if(college.equals("000279")){sexKey.set("国教学院");}
                 else if(college.equals("000289")){sexKey.set("信息商务学院");}
                 else if(college.equals("000359")){sexKey.set("马克思学院");}
                 else if(college.equals("000455")){sexKey.set("中原彼得堡学院");}
                else {
                    return ;
                }
                intValue.set(Integer.parseInt(college));
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
        Job job = new Job(conf, "collegenumber");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, args[0]);
        job.setJarByClass(CollegeNumber.class);
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
