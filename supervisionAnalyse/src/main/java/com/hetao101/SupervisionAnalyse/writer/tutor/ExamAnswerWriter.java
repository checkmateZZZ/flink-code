package com.hetao101.SupervisionAnalyse.writer.tutor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author wangcong
 * @date 2019/10/14
 */
public class ExamAnswerWriter extends RichSinkFunction<ArrayList<Row>> {
    private static Logger logger = Logger.getLogger(ExamAnswerWriter.class);
    Connection writeConn = null;
    PreparedStatement writePs = null;

    //初始化 建立插入连接
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Class.forName(ConfigManager.getProperty(Constants.MYSQL_DRIVER));
        Properties mysqlProp = new Properties();
        mysqlProp.setProperty("user", ConfigManager.getProperty(Constants.MYSQL_USER_TEST));
        mysqlProp.setProperty("password",ConfigManager.getProperty(Constants.MYSQL_PASSWORD_TEST));

        writeConn = DriverManager
                .getConnection(ConfigManager.getProperty(Constants.MYSQL_URL_TEST),mysqlProp);
        writePs = writeConn.prepareStatement(
                "insert into htbc_app.app_supervision_course_tutor_user_exam_rt " +
                        "(user_id,class_id,course_id,course_level,unit_id,unit_sequence,chapter_id" +
                        ",submission_id,exam_id,external_id,is_finish_exam,exam_total_score,exam_total_costTime,submit_cnt,question_id,question_number,exam_result,last_submit_time,user_answer,correct_answer,load_time) " +
                        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE " +
                        "submission_id=values(submission_id)," +
                        "exam_id=values(submission_id)," +
                        "external_id=values(external_id)," +
                        "is_finish_exam=values(is_finish_exam)," +
                        "exam_total_score=values(exam_total_score)," +
                        "exam_total_costTime=values(exam_total_costTime)," +
                        "submit_cnt= submit_cnt + values(submit_cnt)," +
                        "question_id=values(question_id)," +
                        "exam_result=values(exam_result)," +
                        "last_submit_time=if(last_submit_time<values(last_submit_time),values(last_submit_time),last_submit_time)," +
                        "user_answer=values(user_answer)," +
                        "correct_answer=values(correct_answer),"+
                        "load_time=values(load_time);"
        );
    }

    //调用计算后的数据流
    @Override
    public void invoke(ArrayList<Row> values, Context context) throws Exception {
        try{
            for (Row value : values) {
                writePs.setInt(1, (Integer) value.getField(0));
                writePs.setInt(2, (Integer) value.getField(1));
                writePs.setInt(3, (Integer) value.getField(2));
                writePs.setInt(4, (Integer) value.getField(3));
                writePs.setInt(5, (Integer) value.getField(4));
                writePs.setInt(6, (Integer) value.getField(5));
                writePs.setInt(7, (Integer) value.getField(6));
                writePs.setInt(8, (Integer) value.getField(7));
                writePs.setInt(9, (Integer) value.getField(8));
                writePs.setInt(10, (Integer) value.getField(9));
                writePs.setInt(11, (Integer) value.getField(10));
                writePs.setInt(12, (Integer) value.getField(11));
                writePs.setInt(13, (Integer) value.getField(12));
                writePs.setInt(14, (Integer) value.getField(13));
                writePs.setInt(15, (Integer) value.getField(14));
                writePs.setInt(16, (Integer) value.getField(15));
                writePs.setInt(17, (Integer) value.getField(16));
                writePs.setString(18, (String) value.getField(17));
                writePs.setString(19, (String) value.getField(18));
                writePs.setString(20, (String) value.getField(19));
                writePs.setString(21, (String) value.getField(20));

                //遍历插入
                writePs.executeUpdate();
            }
        }
        catch (Exception e){
            logger.info("数据插入异常：" + e);
            logger.info("插入内容：" + values.toString());
        }
    }

    //关闭连接
    @Override
    public void close() {
        try {
            if (writePs!=null) {
                writePs.close();
            }
            if (writeConn!=null) {
                writeConn.close();
            }
        } catch (Exception e) {
            logger.info("关闭连接异常：" + e);
        }
    }
}
