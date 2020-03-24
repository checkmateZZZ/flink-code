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
public class CorrectInfoWriter extends RichSinkFunction<Row> {
    private static Logger logger = Logger.getLogger(CorrectInfoWriter.class);
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
                        ",is_homework_remark,homework_score" +
                        ",load_time) " +
                        "values (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE " +
                        "is_homework_remark=if(values(is_homework_remark)!=0,1,is_homework_remark)," +
                        "homework_score=if(homework_score is not null and homework_score>values(homework_score),homework_score,values(homework_score))," +
                        "load_time=values(load_time);"
        );
    }

    //调用计算后的数据流
    @Override
    public void invoke(Row value, Context context) {
        try {
            writePs.setInt(1, (Integer) value.getField(0));
            writePs.setInt(2, (Integer) value.getField(1));
            writePs.setInt(3, (Integer) value.getField(2));
            writePs.setInt(4, (Integer) value.getField(3));
            writePs.setInt(5, (Integer) value.getField(4));
            writePs.setString(6, (String) value.getField(5));

            //执行插入
            writePs.executeUpdate();
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
