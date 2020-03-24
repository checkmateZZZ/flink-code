package com.hetao101.SupervisionAnalyse.writer;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author wangcong
 * @date 2019/10/14
 */
public class ApplyReportPushWriter extends RichSinkFunction<Tuple3<Integer,Integer,Integer>> {

    Connection writeConn = null;
    PreparedStatement writePs = null;

    //初始化 建立插入连接
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        writeConn = DriverManager
                .getConnection("jdbc:mysql://datacenterdb.testing.pipacoding.com:3306/htbc_app?characterEncoding=utf-8&useSSL=true", "root", "tfeGNtOhoxTk6c1I");
        writePs = writeConn.prepareStatement(
                "insert into htbc_app.app_apply_report_push_rt (" +
                "term_id" +
                ",counselor_id" +
                ",apply_cnt" +
                ") value (?,?,?) ON DUPLICATE KEY UPDATE apply_cnt=apply_cnt+VALUES(apply_cnt)"
        );

    }

    //调用计算后的数据流
    @Override
    public void invoke(Tuple3<Integer,Integer,Integer> value, Context context) throws Exception {

            writePs.setInt(1,value.f0);
            writePs.setInt(2,value.f1);
            writePs.setInt(3,value.f2);

            //执行插入
            writePs.executeUpdate();

    }

    //关闭连接
    @Override
    public void close() throws Exception {
        try {
            if (writePs != null) {
                writePs.close();
            }
            if (writeConn != null) {
                writeConn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
