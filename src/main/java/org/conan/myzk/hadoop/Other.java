package org.conan.myzk.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

public class Other {

    public static String file = "logfile/biz/other.csv";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    private static String month = "2013-01";

    public static void main(String[] args) throws IOException {
        calcOther(file);
    }

    public static int calcOther(String file) throws IOException {
        int money = 0;
        BufferedReader br = new BufferedReader(new FileReader(new File(file)));

        String s = null;
        while ((s = br.readLine()) != null) {
            // System.out.println(s);
            String[] tokens = DELIMITER.split(s);
            if (tokens[0].startsWith(month)) {// 1月的数据
                money += Integer.parseInt(tokens[1]);
            }
        }
        br.close();

        System.out.println("Output:" + month + "," + money);
        return money;
    }
}
