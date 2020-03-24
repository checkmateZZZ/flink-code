package com.hetao101.SupervisionAnalyse.writer.counselor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import com.hetao101.SupervisionAnalyse.entity.MainPlotUser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Properties;

import static com.hetao101.SupervisionAnalyse.util.TimeUtil.parseLong2StringNew;

/**
 * @author wangcong
 * @date 2019/10/14
 */
public class UnlockCntWriter extends RichSinkFunction<ArrayList<MainPlotUser>> {
    private static Logger logger = Logger.getLogger(UnlockCntWriter.class);
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
        writePs = writeConn.prepareStatement("replace into htbc_app.app_supervision_course_user_learning_stat_rt (" +
                "user_id" +
                ",class_id" +
                ",class_name" +
                ",grade" +
                ",class_type" +
                ",term_id" +
                ",term_name" +
                ",counselor_id" +
                ",counselor_name" +
                ",course_level" +
                ",unit_id" +
                ",unit_name" +
                ",unit_sequence" +
                ",unit_unlocked_time" +
                ",homework_unlock_cnt" +
                ",challenge_unlock_cnt" +
                ",total_unlock_cnt" +
                ",load_time" +
                ")value (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

    }

    //调用计算后的数据流
    @Override
    public void invoke(ArrayList<MainPlotUser> value, Context context) {
        try {
            for (MainPlotUser mainPlotUser : value) {
                writePs.setInt(1, mainPlotUser.getUserId());
                writePs.setInt(2, mainPlotUser.getClassId());
                writePs.setString(3, mainPlotUser.getClassName());
                writePs.setInt(4, mainPlotUser.getGrade());
                writePs.setInt(5, mainPlotUser.getClassType());
                writePs.setInt(6, mainPlotUser.getTermId());
                writePs.setString(7, mainPlotUser.getTermName());
                writePs.setInt(8, mainPlotUser.getCounselorId());
                writePs.setString(9, mainPlotUser.getCounselorName());
                writePs.setInt(10,mainPlotUser.getCourseLevel());
                writePs.setInt(11, mainPlotUser.getUnitId());
                writePs.setString(12, mainPlotUser.getUnitName());
                writePs.setInt(13, mainPlotUser.getUnitSequence());
                writePs.setString(14, mainPlotUser.getUnitUnlockedTime());
                writePs.setInt(15, mainPlotUser.getHomeworkOpenCnt());
                writePs.setInt(16, mainPlotUser.getChallengeOpenCnt());
                writePs.setInt(17, mainPlotUser.getTotalOpenCnt());
                writePs.setString(18, parseLong2StringNew(System.currentTimeMillis()));

                //执行插入
                writePs.executeUpdate();
            }
        } catch (Exception e) {
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
