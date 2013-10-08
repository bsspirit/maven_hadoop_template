package org.conan.myhadoop.mr.kpi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/*
 * KPI Object
 */
public class KPI {
    private String remote_addr;// 记录客户端的ip地址
    private String remote_user;// 记录客户端用户名称,忽略属性"-"
    private String time_local;// 记录访问时间与时区
    private String request;// 记录请求的url与http协议
    private String status;// 记录请求状态；成功是200
    private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
    private String http_referer;// 用来记录从那个页面链接访问过来的
    private String http_user_agent;// 记录客户浏览器的相关信息

    private boolean valid = true;// 判断数据是否合法

    private static KPI parser(String line) {
        System.out.println(line);
        KPI kpi = new KPI();
        String[] arr = line.split(" ");
        if (arr.length > 11) {
            kpi.setRemote_addr(arr[0]);
            kpi.setRemote_user(arr[1]);
            kpi.setTime_local(arr[3].substring(1));
            kpi.setRequest(arr[6]);
            kpi.setStatus(arr[8]);
            kpi.setBody_bytes_sent(arr[9]);
            kpi.setHttp_referer(arr[10]);
            
            if (arr.length > 12) {
                kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
            } else {
                kpi.setHttp_user_agent(arr[11]);
            }

            if (Integer.parseInt(kpi.getStatus()) >= 400) {// 大于400，HTTP错误
                kpi.setValid(false);
            }
        } else {
            kpi.setValid(false);
        }
        return kpi;
    }

    /**
     * 按page的pv分类
     */
    public static KPI filterPVs(String line) {
        KPI kpi = parser(line);
        Set<String> pages = new HashSet<String>();
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");

        if (!pages.contains(kpi.getRequest())) {
            kpi.setValid(false);
        }
        return kpi;
    }

    /**
     * 按page的独立ip分类
     */
    public static KPI filterIPs(String line) {
        KPI kpi = parser(line);
        Set<String> pages = new HashSet<String>();
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");

        if (!pages.contains(kpi.getRequest())) {
            kpi.setValid(false);
        }
        
        return kpi;
    }

    /**
     * PV按浏览器分类
     */
    public static KPI filterBroswer(String line) {
        return parser(line);
    }
    
    /**
     * PV按小时分类
     */
    public static KPI filterTime(String line) {
        return parser(line);
    }
    
    /**
     * PV按访问域名分类
     */
    public static KPI filterDomain(String line){
        return parser(line);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("valid:" + this.valid);
        sb.append("\nremote_addr:" + this.remote_addr);
        sb.append("\nremote_user:" + this.remote_user);
        sb.append("\ntime_local:" + this.time_local);
        sb.append("\nrequest:" + this.request);
        sb.append("\nstatus:" + this.status);
        sb.append("\nbody_bytes_sent:" + this.body_bytes_sent);
        sb.append("\nhttp_referer:" + this.http_referer);
        sb.append("\nhttp_user_agent:" + this.http_user_agent);
        return sb.toString();
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public Date getTime_local_Date() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
        return df.parse(this.time_local);
    }
    
    public String getTime_local_Date_hour() throws ParseException{
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
        return df.format(this.getTime_local_Date());
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }
    
    public String getHttp_referer_domain(){
        if(http_referer.length()<8){ 
            return http_referer;
        }
        
        String str=this.http_referer.replace("\"", "").replace("http://", "").replace("https://", "");
        return str.indexOf("/")>0?str.substring(0, str.indexOf("/")):str;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public static void main(String args[]) {
        String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"";
        System.out.println(line);
        KPI kpi = new KPI();
        String[] arr = line.split(" ");

        kpi.setRemote_addr(arr[0]);
        kpi.setRemote_user(arr[1]);
        kpi.setTime_local(arr[3].substring(1));
        kpi.setRequest(arr[6]);
        kpi.setStatus(arr[8]);
        kpi.setBody_bytes_sent(arr[9]);
        kpi.setHttp_referer(arr[10]);
        kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
        System.out.println(kpi);

        try {
            SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd:HH:mm:ss", Locale.US);
            System.out.println(df.format(kpi.getTime_local_Date()));
            System.out.println(kpi.getTime_local_Date_hour());
            System.out.println(kpi.getHttp_referer_domain());
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
