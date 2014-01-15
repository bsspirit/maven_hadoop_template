package org.conan.myhadoop.matrix;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.conan.myhadoop.hdfs.HdfsDAO;

public class MartrixMultiply {

    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;// m1 or m2

        private int rowNum = 2;// 矩阵A的行数
        private int colNum = 2;// 矩阵B的列数
        private int rowIndexA = 1; // 矩阵A，当前在第几行
        private int rowIndexB = 1; // 矩阵B，当前在第几行

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();// 判断读的数据集
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = MainRun.DELIMITER.split(values.toString());
            if (flag.equals("m1")) {
                for (int i = 1; i <= rowNum; i++) {
                    Text k = new Text(rowIndexA + "," + i);
                    for (int j = 1; j <= tokens.length; j++) {
                        Text v = new Text("A:" + j + "," + tokens[j - 1]);
                        context.write(k, v);
                        System.out.println(k.toString() + "  " + v.toString());
                    }

                }
                rowIndexA++;

            } else if (flag.equals("m2")) {
                for (int i = 1; i <= tokens.length; i++) {
                    for (int j = 1; j <= colNum; j++) {
                        Text k = new Text(i + "," + j);
                        Text v = new Text("B:" + rowIndexB + "," + tokens[j - 1]);
                        context.write(k, v);
                        System.out.println(k.toString() + "  " + v.toString());
                    }
                }

                rowIndexB++;
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            System.out.print(key.toString() + ":");

            for (Text line : values) {
                String val = line.toString();
                System.out.print("("+val+")");

                if (val.startsWith("A:")) {
                    String[] kv = MainRun.DELIMITER.split(val.substring(2));
                    mapA.put(kv[0], kv[1]);

                    // System.out.println("A:" + kv[0] + "," + kv[1]);

                } else if (val.startsWith("B:")) {
                    String[] kv = MainRun.DELIMITER.split(val.substring(2));
                    mapB.put(kv[0], kv[1]);

                    // System.out.println("B:" + kv[0] + "," + kv[1]);
                }
            }

            int result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();
                result += Integer.parseInt(mapA.get(mapk)) * Integer.parseInt(mapB.get(mapk));
            }
            context.write(key, new IntWritable(result));
            System.out.println();

            // System.out.println("C:" + key.toString() + "," + result);
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = MainRun.config();

        String input = path.get("input");
        String input1 = path.get("input1");
        String input2 = path.get("input2");
        String output = path.get("output");

        HdfsDAO hdfs = new HdfsDAO(MainRun.HDFS, conf);
        hdfs.rmr(input);
        hdfs.mkdirs(input);
        hdfs.copyFile(path.get("m1"), input1);
        hdfs.copyFile(path.get("m2"), input2);

        Job job = new Job(conf);
        job.setJarByClass(MartrixMultiply.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
