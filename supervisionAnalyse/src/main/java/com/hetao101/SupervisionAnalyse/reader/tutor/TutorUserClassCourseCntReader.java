package com.hetao101.SupervisionAnalyse.reader.tutor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

import static com.hetao101.SupervisionAnalyse.util.TimeUtil.parseLong2StringNew;

/**
 * @author wangcong
 * @date 2019/10/14
 */
public class TutorUserClassCourseCntReader extends RichSourceFunction<ArrayList<Row>> {

    private Connection myConnect = null;
    private PreparedStatement userClassCourseCntPS = null;
    private ResultSet rs = null;
    private Boolean isRunning;
    private String CurrentYear = parseLong2StringNew(System.currentTimeMillis(),"yyyy-MM");
    private String CurrentParDate = parseLong2StringNew(System.currentTimeMillis(),"yyyyMMdd");



    //初始化 建立读取连接
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning=true;
        Class.forName(ConfigManager.getProperty(Constants.HIVE_DRIVER));
        Properties hiveProp = new Properties();
        hiveProp.setProperty("user",ConfigManager.getProperty(Constants.HIVE_USER));
        hiveProp.setProperty("password",ConfigManager.getProperty(Constants.HIVE_PASSWORD));

        myConnect = DriverManager
                .getConnection(ConfigManager.getProperty(Constants.HIVE_URL),hiveProp);

        userClassCourseCntPS = myConnect
                .prepareStatement("select user_id" +
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
                        ",count(distinct if(finish_tag=1,chapter_id,null)) as homework_unlocked_cnt " +
                        ",count(distinct if(tag=0 and item_type in ('PROJECT','EXAM'),chapter_id,null)) as challenge_unlocked_cnt" +
                        ",count(distinct if(tag=0,chapter_id,null)) as total_unlocked_cnt" +
                        ",load_time " +
                        "from htbc_dw.dw_user_class_course_detail_plot where class_type in (5,7,10,11) and substr(unit_unlocked_time,1,7)=? and par_date in (?)" +
                        "group by user_id" +
                        ",class_id"+
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
                        ",load_time");

    }

    //将读到的结果输入数据流
    @Override
    public void run(SourceContext<ArrayList<Row>> ctx) throws Exception {
        //将结果存为 <class_id,<对应课程>>
        ArrayList<Row> userClassCourseCnts = new ArrayList<>();
        while (isRunning) {
            userClassCourseCntPS.setString(1,CurrentYear);
            userClassCourseCntPS.setString(2,CurrentParDate);

            rs = userClassCourseCntPS.executeQuery();
            while (rs.next()) {
                Row row = new Row(18);

                row.setField(0,rs.getInt(1));
                row.setField(1,rs.getInt(2));
                row.setField(2,rs.getString(3));
                row.setField(3,rs.getInt(4));
                row.setField(4,rs.getInt(5));
                row.setField(5,rs.getInt(6));
                row.setField(6,rs.getString(7));
                row.setField(7,rs.getInt(8));
                row.setField(8,rs.getString(9));
                row.setField(9,rs.getInt(10));
                row.setField(10,rs.getInt(11));
                row.setField(11,rs.getString(12));
                row.setField(12,rs.getInt(13));
                row.setField(13,rs.getString(14));
                row.setField(14,rs.getInt(15));
                row.setField(15,rs.getInt(16));
                row.setField(16,rs.getInt(17));
                row.setField(17,rs.getInt(18));

                userClassCourseCnts.add(row);
            }
            //发送查询结果
            System.out.println("发送数据");
            ctx.collect(userClassCourseCnts);
            userClassCourseCnts.clear();
            Thread.sleep(1800000);
        }
    }

    //关闭连接
    @Override
    public void cancel() {
        try {
            if(rs != null) {
                rs.close();
            }
            if(userClassCourseCntPS!=null){
                userClassCourseCntPS.close();
            }
            if(myConnect!=null){
                myConnect.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        isRunning=false;
    }
}