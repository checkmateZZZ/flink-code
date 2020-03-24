package com.hetao101.SupervisionAnalyse.reader;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author wangcong
 * @date 2019/10/22
 */
//读取课程树表
public class ApplyReportPushReader extends RichSourceFunction<ArrayList<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>> {
    private Connection myConnect = null;
    private PreparedStatement classCourseAllPlotPS = null;
    volatile private Boolean isRunning;



    //初始化 建立读取连接
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning=true;

        //mysql
//        Class.forName("com.mysql.jdbc.Driver");
//        myConnect = DriverManager
//                .getConnection("jdbc:mysql://datacenterdb.testing.pipacoding.com:3306/htbc_app?characterEncoding=utf-8&useSSL=true", "root", "tfeGNtOhoxTk6c1I");


        //presto
//        Properties properties = new Properties();
//        String url="jdbc:presto://10.100.7.251:9090/hive/default";
//        properties.setProperty("user","wangcong");
        //Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        //myConnect = DriverManager.getConnection(url,properties);


        //hive
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Properties prop = new Properties();
        prop.setProperty("user","wangcong");
        String url = "jdbc:hive2://10.100.7.251:10000/htbc_dw";
        Connection myConnect = DriverManager.getConnection(url, prop);


        classCourseAllPlotPS = myConnect.prepareStatement("select " +
                "distinct " +
                "term_id" +
                ",user_id" +
                ",counselor" +
                ",class_id" +
                ",class_type" +
                ",user_status " +
                "from htbc_dw.dw_user_class_detail " +
                "where user_status=1 and class_type in (1,4,8,9) and par_date=20191202");
    }

    //将读到的结果输入数据流
    @Override
    public void run(SourceContext<ArrayList<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>> ctx) throws Exception {

        while (isRunning) {
            ArrayList<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> userClassInfos = new ArrayList<>();
            ResultSet rs = classCourseAllPlotPS.executeQuery();
            while (rs.next()) {
                Tuple6 userClassInfo = new Tuple6(rs.getInt(1)
                        , rs.getInt(2)
                        , rs.getInt(3)
                        , rs.getInt(4)
                        , rs.getInt(5)
                        , rs.getInt(6));

                userClassInfos.add(userClassInfo);
            }
            //发送查询结果
            ctx.collect(userClassInfos);
            Thread.sleep(60000);
            isRunning=false;
        }
    }

    //关闭连接
    @Override
    public void cancel() {

        try {
            if (classCourseAllPlotPS != null) {
                classCourseAllPlotPS.close();
            }
            if (myConnect != null) {
                myConnect.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
