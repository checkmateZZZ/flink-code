package com.hetao101.SupervisionAnalyse.reader.tutor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

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
public class TutorChapterTypeInfoReader extends RichSourceFunction<HashMap<Integer, HashMap<Integer, Tuple2>>> {
    private Connection myConnect = null;
    private PreparedStatement classCourseAllPlotPS = null;
    private ResultSet rs = null;
    private Boolean isRunning;
    private String CurrentYear = parseLong2StringNew(System.currentTimeMillis(),"yyyy");
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


        classCourseAllPlotPS = myConnect.prepareStatement("select " +
                "distinct " +
                "chapter_id" +
                ",unit_id" +
                ",item_type" +
                ",tag" +
                " from htbc_dw.dw_class_course_all_plot" +
                " where class_type in (5,7,10,11) " +
                "and unit_unlocked_time>=? " +
                "and chapter_id is not null " +
                "and tag is not null " +
                "and par_date in (?)");
    }

    //将读到的结果输入数据流
    @Override
    public void run(SourceContext<HashMap<Integer, HashMap<Integer, Tuple2>>> ctx) throws Exception {

        //将结果存为 <unitId,<<chapterId,<itemType,tag>>>>
        HashMap<Integer, HashMap<Integer, Tuple2>> chapterTypeInfos = new HashMap<>();

        while (isRunning) {
            classCourseAllPlotPS.setString(1,CurrentYear);
            classCourseAllPlotPS.setString(2,CurrentParDate);
            rs = classCourseAllPlotPS.executeQuery();

            while (rs.next()) {
                if (chapterTypeInfos.containsKey(rs.getInt(2))){
                    chapterTypeInfos
                            .get(rs.getInt(2))
                            .put(rs.getInt(1),new Tuple2(rs.getString(3),rs.getInt(4)));
                } else {
                    //<chapterId,<itemType,tag>>
                    HashMap<Integer, Tuple2> chapterTypeInfo = new HashMap<>();
                    chapterTypeInfo.put(rs.getInt(1),new Tuple2(rs.getString(3),rs.getInt(4)));
                    chapterTypeInfos.put(rs.getInt(2),chapterTypeInfo);
                }
            }

            //发送查询结果
            ctx.collect(chapterTypeInfos);
            chapterTypeInfos.clear();
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
