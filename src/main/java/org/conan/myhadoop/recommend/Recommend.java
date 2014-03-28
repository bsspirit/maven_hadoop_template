package org.conan.myhadoop.recommend;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;
import org.conan.myhadoop.hdfs.HdfsDAO;

public class Recommend {

    public static final String HDFS = "hdfs://192.168.1.210:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) throws Exception {
        Map<String, String> path = new HashMap<String, String>();
        path.put("data", "logfile/small.csv");
        path.put("Step1Input", HDFS + "/user/hdfs/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", path.get("Step1Input") + "/step3_2");
        
        path.put("Step4Input1", path.get("Step3Output1"));
        path.put("Step4Input2", path.get("Step3Output2"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");
        
        path.put("Step5Input1", path.get("Step3Output1"));
        path.put("Step5Input2", path.get("Step3Output2"));
        path.put("Step5Output", path.get("Step1Input") + "/step5");
        
        path.put("Step6Input", path.get("Step5Output"));
        path.put("Step6Output", path.get("Step1Input") + "/step6");
        

        Step1.run(path);
        Step2.run(path);
        Step3.run1(path);
        Step3.run2(path);
//        Step4.run(path);
        
        Step4_Update.run(path);
        Step4_Update2.run(path);
        
        
//        // hadoop fs -cat /user/hdfs/recommend/step4/part-00000
//        JobConf conf = config();
//        HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
//        hdfs.cat("/user/hdfs/recommend/step4/part-00000");
        
        System.exit(0);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(Recommend.class);
        conf.setJobName("Recommand");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.set("io.sort.mb", "1024");
        return conf;
    }

}
