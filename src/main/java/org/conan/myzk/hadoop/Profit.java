package org.conan.myzk.hadoop;

import java.io.IOException;

import org.conan.myhadoop.hdfs.HdfsDAO;

public class Profit {

    public static void main(String[] args) throws Exception {
        profit();
    }

    public static void profit() throws Exception {
        int sell = getSell();
        int purchase = getPurchase();
        int other = getOther();
        int profit = sell - purchase - other;
        System.out.printf("profit = sell - purchase - other = %d - %d - %d = %d\n", sell, purchase, other, profit);
    }

    public static int getPurchase() throws Exception {
        HdfsDAO hdfs = new HdfsDAO(Purchase.HDFS, Purchase.config());
        return Integer.parseInt(hdfs.cat(Purchase.path().get("output") + "/part-r-00000").trim());
    }

    public static int getSell() throws Exception {
        HdfsDAO hdfs = new HdfsDAO(Sell.HDFS, Sell.config());
        return Integer.parseInt(hdfs.cat(Sell.path().get("output") + "/part-r-00000").trim());
    }

    public static int getOther() throws IOException {
        return Other.calcOther(Other.file);
    }

}
