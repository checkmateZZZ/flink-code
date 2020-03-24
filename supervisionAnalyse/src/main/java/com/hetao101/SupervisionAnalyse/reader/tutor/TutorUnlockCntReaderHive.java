package com.hetao101.SupervisionAnalyse.reader.tutor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import com.hetao101.SupervisionAnalyse.entity.MainPlot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import static com.hetao101.SupervisionAnalyse.util.TimeUtil.parseLong2StringNew;

/**
 * @author wangcong
 * @date 2019/10/14
 */
public class TutorUnlockCntReaderHive extends RichSourceFunction<HashMap<Integer, ArrayList<MainPlot>>> {

    private Connection myConnect = null;
    private PreparedStatement unlockCntPS = null;
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
        hiveProp.setProperty("user",ConfigManager.getProperty(Constants.HIVE_USER));
        hiveProp.setProperty("password",ConfigManager.getProperty(Constants.HIVE_PASSWORD));

        myConnect = DriverManager
                .getConnection(ConfigManager.getProperty(Constants.HIVE_URL),hiveProp);

        unlockCntPS = myConnect
                .prepareStatement("select class_id" +
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
                        ",count(distinct if(tag=0,chapter_id,null)) as total_unlocked_cnt " +
                        ",course_package_id " +
                        "from htbc_dw.dw_class_course_all_plot where class_type in (5,7,10,11) and unit_unlocked_time>=? and par_date in (?)" +
                        "group by class_id"+
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
                        ",course_package_id");

    }

    //将读到的结果输入数据流
    @Override
    public void run(SourceContext<HashMap<Integer, ArrayList<MainPlot>>> ctx) throws Exception {

        //将结果存为 <class_id,<对应课程>>
        HashMap<Integer, ArrayList<MainPlot>> classMainPlots = new HashMap<>();
        while (isRunning) {
            unlockCntPS.setString(1,CurrentYear);
            unlockCntPS.setString(2,CurrentParDate);

            rs = unlockCntPS.executeQuery();
            while (rs.next()) {
                MainPlot mainPlot = new MainPlot();

                mainPlot.setClassId(rs.getInt(1));
                mainPlot.setClassName(rs.getString(2));
                mainPlot.setGrade(rs.getInt(3));
                mainPlot.setClassType(rs.getInt(4));
                mainPlot.setTermId(rs.getInt(5));
                mainPlot.setTermName(rs.getString(6));
                mainPlot.setCounselorId(rs.getInt(7));
                mainPlot.setCounselorName(rs.getString(8));
                mainPlot.setCourseLevel(rs.getInt(9));
                mainPlot.setUnitId(rs.getInt(10));
                mainPlot.setUnitName(rs.getString(11));
                mainPlot.setUnitSequence(rs.getInt(12));
                mainPlot.setUnitUnlockedTime(rs.getString(13));
                mainPlot.setHomeworkOpenCnt(rs.getInt(14));
                mainPlot.setChallengeOpenCnt(rs.getInt(15));
                mainPlot.setTotalOpenCnt(rs.getInt(16));
                mainPlot.setCoursePackageId(rs.getInt(17));


                //相同班级放入同一集合
                if (classMainPlots.containsKey(mainPlot.getClassId())){
                    classMainPlots.get(mainPlot.getClassId()).add(mainPlot);
                }else{//不同班纳放入新集合
                    ArrayList<MainPlot> mainPlots = new ArrayList<>();
                    mainPlots.add(mainPlot);
                    classMainPlots.put(mainPlot.getClassId(),mainPlots);
                }
            }
            //发送查询结果
            ctx.collect(classMainPlots);
            //mainPlots.clear();
            classMainPlots.clear();
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
            if(unlockCntPS!=null){
                unlockCntPS.close();
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