package com.hetao101.SupervisionAnalyse.reader;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Properties;

import static com.hetao101.SupervisionAnalyse.util.TimeUtil.parseLong2StringNew;

/**
 * @author wangcong
 * @date 2019/10/22
 */
//读取课程树表
public class ClassInfoReader extends RichSourceFunction<HashMap<Integer, Row>> {
    private Connection myConnect = null;
    private PreparedStatement classInfoPS = null;
    private ResultSet rs = null;
    private Boolean isRunning;
    //private String CurrentYear = parseLong2StringNew(System.currentTimeMillis(),"yyyy");
    private String CurrentParDate = parseLong2StringNew(System.currentTimeMillis(),"yyyyMMdd");




    //初始化 建立读取连接
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning=true;
        Class.forName(ConfigManager.getProperty(Constants.HIVE_DRIVER));
        Properties hiveProp = new Properties();
        hiveProp.setProperty("user", ConfigManager.getProperty(Constants.HIVE_USER));
        hiveProp.setProperty("password",ConfigManager.getProperty(Constants.HIVE_PASSWORD));


        myConnect = DriverManager
                .getConnection(ConfigManager.getProperty(Constants.HIVE_URL),hiveProp);


        classInfoPS = myConnect.prepareStatement("select " +
                "class_id" +
                ",class_type" +
                ",course_package_id" +
                ",grade" +
                ",course_group" +
                ",course_type" +
                ",course_package_phase" +
                ",classopen_time" +
                " from htbc_dw.dim_class" +
                " where par_date in (?)");
    }

    //将读到的结果输入数据流
    @Override
    public void run(SourceContext<HashMap<Integer, Row>> ctx) throws Exception {

        //将结果存为 <unitId,<<chapterId,<itemType,tag>>>>
        HashMap<Integer, Row> classInfos = new HashMap<>();

        while (isRunning) {
            classInfoPS.setString(1,CurrentParDate);

            rs = classInfoPS.executeQuery();
            while (rs.next()) {
                Row row = new Row(7);
                row.setField(0,rs.getInt(2));
                row.setField(1,rs.getInt(3));
                row.setField(2,rs.getString(4));
                row.setField(3,rs.getInt(5));
                row.setField(4,rs.getString(6));
                row.setField(5,rs.getInt(7));
                row.setField(6,rs.getString(8));
                classInfos.put(rs.getInt(1),row);

            }

            //发送查询结果
            ctx.collect(classInfos);
            classInfos.clear();
            Thread.sleep(1800000);
        }
    }

    //关闭连接
    @Override
    public void cancel() {
        try {
            if (rs != null) {
                rs.close();
            }
            if (classInfoPS != null) {
                classInfoPS.close();
            }
            if (myConnect != null) {
                myConnect.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        isRunning=false;
    }

}
