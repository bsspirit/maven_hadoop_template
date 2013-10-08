package org.conan.myhadoop.mr.kpi;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class KPITime {

    public static class KPITimeMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {
                try {
                    word.set(kpi.getTime_local_Date_hour());
                    output.collect(word, one);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class KPITimeReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://192.168.1.210:9000/user/hdfs/log_kpi";
        String output = "hdfs://192.168.1.210:9000/user/hdfs/log_kpi/time";

        JobConf conf = new JobConf(KPITime.class);
        conf.setJobName("KPITime");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(KPITimeMapper.class);
        conf.setCombinerClass(KPITimeReducer.class);
        conf.setReducerClass(KPITimeReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }

}
