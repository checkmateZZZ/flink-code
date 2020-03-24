package com.hetao101.SupervisionAnalyse.writer.counselor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author wangcong
 * @date 2019/10/14
 */
public class FinishCntWriter extends RichSinkFunction<Row> {
    private static Logger logger = Logger.getLogger(FinishCntWriter.class);
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
                "insert into htbc_app.app_supervision_course_user_learning_stat_rt " +
                        "(class_id,user_id,unit_id" +
                        ",class_name,grade,class_type,term_id,term_name,counselor_id,counselor_name,course_level,unit_name,unit_sequence,unit_unlocked_time,homework_unlock_cnt,challenge_unlock_cnt,total_unlock_cnt" +
                        ",is_attendance,attendance_time" +
                        ",challenge_finish_cnt,first_finish_challenge_time,last_finish_challenge_time" +
                        ",total_finish_cnt,first_finish_total_time,last_finish_total_time" +
                        ",load_time) " +
                        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE " +
                        "class_name=values(class_name)," +
                        "grade=values(grade)," +
                        "class_type=values(class_type)," +
                        "term_id=values(term_id)," +
                        "term_name=values(term_name)," +
                        "counselor_id=values(counselor_id)," +
                        "counselor_name=values(counselor_name)," +
                        "course_level=values(course_level)," +
                        "unit_name=values(unit_name)," +
                        "unit_sequence=values(unit_sequence)," +
                        "unit_unlocked_time=values(unit_unlocked_time)," +
                        "homework_unlock_cnt=values(homework_unlock_cnt)," +
                        "challenge_unlock_cnt=values(challenge_unlock_cnt)," +
                        "total_unlock_cnt=values(total_unlock_cnt)," +
                        "is_attendance=if(values(is_attendance)!=0,1,is_attendance)," +
                        "attendance_time=if(((attendance_time is not null and attendance_time>values(attendance_time)) or attendance_time is null),values(attendance_time),attendance_time)," +
                        "challenge_finish_cnt=challenge_finish_cnt+values(challenge_finish_cnt)," +
                        "first_finish_challenge_time=if((first_finish_challenge_time>values(first_finish_challenge_time) or first_finish_challenge_time is null),values(first_finish_challenge_time),first_finish_challenge_time)," +
                        "last_finish_challenge_time=if((last_finish_challenge_time<values(last_finish_challenge_time) or last_finish_challenge_time is null),values(last_finish_challenge_time),last_finish_challenge_time)," +
                        "total_finish_cnt=total_finish_cnt+values(total_finish_cnt)," +
                        "first_finish_total_time=if((first_finish_total_time>values(first_finish_total_time) or first_finish_total_time is null),values(first_finish_total_time),first_finish_total_time)," +
                        "last_finish_total_time=if((last_finish_total_time<values(last_finish_total_time) or last_finish_total_time is null),values(last_finish_total_time),last_finish_total_time)," +
                        "load_time=values(load_time);"

        );
    }

    //调用计算后的数据流
    @Override
    public void invoke(Row value, Context context) throws Exception {
        try {
            writePs.setInt(1, (Integer) value.getField(0));
            writePs.setInt(2, (Integer) value.getField(1));
            writePs.setInt(3, (Integer) value.getField(2));
            writePs.setString(4, (String) value.getField(3));
            writePs.setInt(5, (Integer) value.getField(4));
            writePs.setInt(6, (Integer) value.getField(5));
            writePs.setInt(7, (Integer) value.getField(6));
            writePs.setString(8, (String) value.getField(7));
            writePs.setInt(9, (Integer) value.getField(8));
            writePs.setString(10, (String) value.getField(9));
            writePs.setInt(11, (Integer) value.getField(10));
            writePs.setString(12, (String) value.getField(11));
            writePs.setInt(13, (Integer) value.getField(12));
            writePs.setString(14, (String) value.getField(13));
            writePs.setInt(15, (Integer) value.getField(14));
            writePs.setInt(16, (Integer) value.getField(15));
            writePs.setInt(17, (Integer) value.getField(16));
            writePs.setInt(18, (Integer) value.getField(17));
            writePs.setString(19, (String) value.getField(18));
            writePs.setInt(20, (Integer) value.getField(19));
            writePs.setString(21, (String) value.getField(20));
            writePs.setString(22, (String) value.getField(21));
            writePs.setInt(23, (Integer) value.getField(22));
            writePs.setString(24, (String) value.getField(23));
            writePs.setString(25, (String) value.getField(24));
            writePs.setString(26, (String) value.getField(25));

            //执行插入
            writePs.executeUpdate();
        } catch (Exception e){
            logger.info("数据插入异常：" + e);
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
