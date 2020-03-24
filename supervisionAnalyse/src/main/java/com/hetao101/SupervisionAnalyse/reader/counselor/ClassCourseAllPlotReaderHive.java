package com.hetao101.SupervisionAnalyse.reader.counselor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import com.hetao101.SupervisionAnalyse.entity.ClassCourseAllPlot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

import static com.hetao101.SupervisionAnalyse.util.TimeUtil.parseLong2StringNew;

/**
 * @author wangcong
 * @date 2019/10/22
 */
//读取课程树表
public class ClassCourseAllPlotReaderHive extends RichSourceFunction<ArrayList<ClassCourseAllPlot>> {
    private Connection myConnect = null;
    private PreparedStatement classCourseAllPlotPS = null;
    private ResultSet rs = null;
    private Boolean isRunning;
    private String CurrentParDate = parseLong2StringNew(System.currentTimeMillis(),"yyyyMMdd");
    private String CurrentYear = parseLong2StringNew(System.currentTimeMillis(),"yyyy");



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
                "term_id" +
                ",term_name" +
                ",counselor_id" +
                ",counselor_name" +
                ",class_id" +
                ",class_name" +
                ",grade" +
                ",classopen_time" +
                ",class_type" +
                ",course_type" +
                ",course_id" +
                ",course_level" +
                ",unit_id" +
                ",unit_name" +
                ",unit_sequence" +
                ",unit_locked" +
                ",unit_unlocked_time" +
                ",chapter_id" +
                ",chapter_name" +
                ",item_type" +
                ",tag" +
                ",finish_tag" +
                " from htbc_dw.dw_class_course_all_plot" +
                " where class_type in (4,8,9) and classopen_time>=? and par_date in (?)");
    }

    //将读到的结果输入数据流
    @Override
    public void run(SourceContext<ArrayList<ClassCourseAllPlot>> ctx) throws Exception {

        //将结果存为 <ClassCourseAllPlot>
        ArrayList<ClassCourseAllPlot> classCourseAllPlots = new ArrayList<>();
        while (isRunning) {
            //logger.info("ClassCourseAllPlotReader:"+System.currentTimeMillis());
            classCourseAllPlotPS.setString(1,CurrentYear);
            classCourseAllPlotPS.setString(2,CurrentParDate);
            rs = classCourseAllPlotPS.executeQuery();
            while (rs.next()) {
                ClassCourseAllPlot classCourseAllPlot = new ClassCourseAllPlot();

                classCourseAllPlot.setTermId(rs.getInt(1));
                classCourseAllPlot.setTermName(rs.getString(2));
                classCourseAllPlot.setCounselorId(rs.getInt(3));
                classCourseAllPlot.setCounselorName(rs.getString(4));
                classCourseAllPlot.setClassId(rs.getInt(5));
                classCourseAllPlot.setClassName(rs.getString(6));
                classCourseAllPlot.setGrade(rs.getInt(7));
                classCourseAllPlot.setClassOpenTime(rs.getString(8));
                classCourseAllPlot.setClassType(rs.getInt(9));
                classCourseAllPlot.setCourseType(rs.getString(10));
                classCourseAllPlot.setCourseId(rs.getInt(11));
                classCourseAllPlot.setCourseLevel(rs.getInt(12));
                classCourseAllPlot.setUnitId(rs.getInt(13));
                classCourseAllPlot.setUnitName(rs.getString(14));
                classCourseAllPlot.setUnitSequence(rs.getInt(15));
                classCourseAllPlot.setUnlocked(rs.getInt(16));
                classCourseAllPlot.setUnitUnlockedTime(rs.getString(17));
                classCourseAllPlot.setChapterId(rs.getInt(18));
                classCourseAllPlot.setChapterName(rs.getString(19));
                classCourseAllPlot.setItemType(rs.getString(20));
                classCourseAllPlot.setTag(rs.getInt(21));
                classCourseAllPlot.setFinishTag(rs.getInt(22));

                classCourseAllPlots.add(classCourseAllPlot);
            }

            //发送查询结果
            ctx.collect(classCourseAllPlots);
            //mainPlots.clear();
            classCourseAllPlots.clear();
            Thread.sleep(300000);
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
