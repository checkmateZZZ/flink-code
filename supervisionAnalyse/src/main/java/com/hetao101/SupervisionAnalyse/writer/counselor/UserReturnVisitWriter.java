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
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author wangcong
 * @date 2019/10/14
 */
public class UserReturnVisitWriter extends RichSinkFunction<ArrayList<Row>> {
    private static Logger logger = Logger.getLogger(UserReturnVisitWriter.class);
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
                        "(class_id,user_id,unit_id,is_return_visit,load_time) " +
                        "values (?,?,?,?,?) ON DUPLICATE KEY UPDATE " +
                        "is_return_visit=values(is_return_visit)," +
                        "load_time=values(load_time);"
        );
    }

    //调用计算后的数据流
    @Override
    public void invoke(ArrayList<Row> value, Context context) {
        try{
            for (Row row : value) {
                writePs.setInt(1, (Integer) row.getField(0));
                writePs.setInt(2, (Integer) row.getField(1));
                writePs.setInt(3, (Integer) row.getField(2));
                writePs.setInt(4, (Integer) row.getField(3));
                writePs.setString(5, (String) row.getField(4));
                //执行插入
                writePs.executeUpdate();
            }
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
