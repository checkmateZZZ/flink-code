//package com.hetao101.SupervisionAnalyse.writer.tutor;
//
//import com.hetao101.SupervisionAnalyse.common.Constants;
//import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.types.Row;
//import org.apache.log4j.Logger;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.util.ArrayList;
//import java.util.Properties;
//
///**
// * @author wangcong
// * @date 2019/10/14
// */
//public class TutorUserClassCourseCntWriter extends RichSinkFunction<ArrayList<Row>> {
//    private static Logger logger = Logger.getLogger(TutorUserClassCourseCntWriter.class);
//    Connection writeConn = null;
//    PreparedStatement writePs = null;
//
//    //初始化 建立插入连接
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//
//        Class.forName(ConfigManager.getProperty(Constants.MYSQL_DRIVER));
//        Properties mysqlProp = new Properties();
//        mysqlProp.setProperty("user", ConfigManager.getProperty(Constants.MYSQL_USER_TEST));
//        mysqlProp.setProperty("password",ConfigManager.getProperty(Constants.MYSQL_PASSWORD_TEST));
//
//        writeConn = DriverManager
//                .getConnection(ConfigManager.getProperty(Constants.MYSQL_URL_TEST),mysqlProp);
//        writePs = writeConn.prepareStatement("replace into htbc_app.app_supervision_course_tutor_user_detail_rt (" +
//                "user_id" +
//                ",class_id" +
//                ",class_name" +
//                ",grade" +
//                ",class_type" +
//                ",term_id" +
//                ",term_name" +
//                ",counselor_id" +
//                ",counselor_name" +
//                ",course_level" +
//                ",unit_id" +
//                ",unit_name" +
//                ",unit_sequence" +
//                ",unit_unlocked_time" +
//                ",homework_unlock_cnt" +
//                ",challenge_unlock_cnt" +
//                ",total_unlock_cnt" +
//                ",load_time" +
//                ")value (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
//
//    }
//
//    //调用计算后的数据流
//    @Override
//    public void invoke(ArrayList<Row> value, Context context) {
//        try {
//            for (Row userClassCourseCnt : value) {
//                writePs.setInt(1, (Integer) userClassCourseCnt.getField(0));
//                writePs.setInt(2, (Integer) userClassCourseCnt.getField(1));
//                writePs.setString(3, (String) userClassCourseCnt.getField(2));
//                writePs.setInt(4, (Integer) userClassCourseCnt.getField(3));
//                writePs.setInt(5, (Integer) userClassCourseCnt.getField(4));
//                writePs.setInt(6, (Integer) userClassCourseCnt.getField(5));
//                writePs.setString(7, (String) userClassCourseCnt.getField(6));
//                writePs.setInt(8, (Integer) userClassCourseCnt.getField(7));
//                writePs.setString(9, (String) userClassCourseCnt.getField(8));
//                writePs.setInt(10, (Integer) userClassCourseCnt.getField(9));
//                writePs.setInt(11, (Integer) userClassCourseCnt.getField(10));
//                writePs.setString(12, (String) userClassCourseCnt.getField(11));
//                writePs.setInt(13, (Integer) userClassCourseCnt.getField(12));
//                writePs.setString(14, (String) userClassCourseCnt.getField(13));
//                writePs.setInt(15, (Integer) userClassCourseCnt.getField(14));
//                writePs.setInt(16, (Integer) userClassCourseCnt.getField(15));
//                writePs.setInt(17, (Integer) userClassCourseCnt.getField(16));
//                writePs.setString(18, (String) userClassCourseCnt.getField(17));
//
//                //执行插入
//                writePs.executeUpdate();
//            }
//        } catch (Exception e) {
//            logger.info("数据插入异常：" + e);
//            logger.info("插入内容：" + value.toString());
//        }
//    }
//
//    //关闭连接
//    @Override
//    public void close() {
//        try {
//            if (writePs!=null) {
//                writePs.close();
//            }
//            if (writeConn!=null) {
//                writeConn.close();
//            }
//        } catch (Exception e) {
//            logger.info("关闭连接异常：" + e);
//        }
//    }
//}
