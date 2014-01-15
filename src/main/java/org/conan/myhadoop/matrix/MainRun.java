package org.conan.myhadoop.matrix;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class MainRun {

    public static final String HDFS = "hdfs://192.168.1.210:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) {
//        martrixMultiply();
        sparseMartrixMultiply();
    }
    
    public static void martrixMultiply() {
        Map<String, String> path = new HashMap<String, String>();
        path.put("m1", "logfile/matrix/m1.csv");// 本地的数据文件
        path.put("m2", "logfile/matrix/m2.csv");
        path.put("input", HDFS + "/user/hdfs/matrix");// HDFS的目录
        path.put("input1", HDFS + "/user/hdfs/matrix/m1");
        path.put("input2", HDFS + "/user/hdfs/matrix/m2");
        path.put("output", HDFS + "/user/hdfs/matrix/output");

        try {
            MartrixMultiply.run(path);// 启动程序
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
    
    public static void sparseMartrixMultiply() {
        Map<String, String> path = new HashMap<String, String>();
        path.put("m1", "logfile/matrix/sm1.csv");// 本地的数据文件
        path.put("m2", "logfile/matrix/sm2.csv");
        path.put("input", HDFS + "/user/hdfs/matrix");// HDFS的目录
        path.put("input1", HDFS + "/user/hdfs/matrix/m1");
        path.put("input2", HDFS + "/user/hdfs/matrix/m2");
        path.put("output", HDFS + "/user/hdfs/matrix/output");

        try {
            SparseMartrixMultiply.run(path);// 启动程序
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static JobConf config() {// Hadoop集群的远程配置信息
        JobConf conf = new JobConf(MainRun.class);
        conf.setJobName("MartrixMultiply");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

}
