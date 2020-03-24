//package com.hetao101.SupervisionAnalyse.reader.counselor;
//
//import com.hetao101.SupervisionAnalyse.common.Constants;
//import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
//import com.hetao101.SupervisionAnalyse.entity.MainPlot;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Properties;
//
///**
// * @author wangcong
// * @date 2019/10/14
// */
//public class UnlockCntReaderPresto extends RichSourceFunction<HashMap<Integer, ArrayList<MainPlot>>> {
//
//    private Connection myConnect = null;
//    private PreparedStatement homeworkCntPS = null;
//    private Boolean isRunning;
//
//
//
//
//    //初始化 建立读取连接
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        isRunning=true;
//        Class.forName(ConfigManager.getProperty(Constants.PRESTO_DRIVER));
//        Properties prestoProp = new Properties();
//        prestoProp.setProperty("user", ConfigManager.getProperty(Constants.PRESTO_USER));
//        prestoProp.setProperty("password",ConfigManager.getProperty(Constants.PRESTO_PASSWORD));
//
//        myConnect = DriverManager
//                .getConnection(ConfigManager.getProperty(Constants.PRESTO_URL), prestoProp);
//
//        homeworkCntPS = myConnect
//                .prepareStatement("select class_id" +
//                        ",class_name" +
//                        ",grade" +
//                        ",class_type" +
//                        ",term_id" +
//                        ",term_name" +
//                        ",counselor_id" +
//                        ",counselor_name" +
//                        ",course_level" +
//                        ",unit_id" +
//                        ",unit_name" +
//                        ",unit_sequence" +
//                        ",unit_unlocked_time" +
//                        ",count(distinct if(finish_tag=1,chapter_id,null)) as homework_unlocked_cnt " +
//                        ",count(distinct if(tag=0 and item_type in ('PROJECT','EXAM'),chapter_id,null)) as challenge_unlocked_cnt" +
//                        ",count(distinct if(tag=0,chapter_id,null)) as total_unlocked_cnt " +
//                        "from htbc_dw.dw_class_course_all_plot where class_type in (4,8,9) and par_date='20200112'" +
//                        "group by class_id"+
//                        ",class_name" +
//                        ",grade" +
//                        ",class_type" +
//                        ",term_id" +
//                        ",term_name" +
//                        ",counselor_id" +
//                        ",counselor_name" +
//                        ",course_level" +
//                        ",unit_id" +
//                        ",unit_name" +
//                        ",unit_sequence" +
//                        ",unit_unlocked_time");
//
//    }
//
//    //将读到的结果输入数据流
//    @Override
//    public void run(SourceContext<HashMap<Integer, ArrayList<MainPlot>>> ctx) throws Exception {
//
//        //将结果存为 <class_id,<对应课程>>
//        HashMap<Integer, ArrayList<MainPlot>> classMainPlots = new HashMap<>();
//        while (isRunning) {
//            //logger.info("UnlockedCntReader:"+System.currentTimeMillis());
//            ResultSet rs = homeworkCntPS.executeQuery();
//            while (rs.next()) {
//                MainPlot mainPlot = new MainPlot();
//
//                mainPlot.setClassId(rs.getInt(1));
//                mainPlot.setClassName(rs.getString(2));
//                mainPlot.setGrade(rs.getInt(3));
//                mainPlot.setClassType(rs.getInt(4));
//                mainPlot.setTermId(rs.getInt(5));
//                mainPlot.setTermName(rs.getString(6));
//                mainPlot.setCounselorId(rs.getInt(7));
//                mainPlot.setCounselorName(rs.getString(8));
//                mainPlot.setCourseLevel(rs.getInt(9));
//                mainPlot.setUnitId(rs.getInt(10));
//                mainPlot.setUnitName(rs.getString(11));
//                mainPlot.setUnitSequence(rs.getInt(12));
//                mainPlot.setUnitUnlockedTime(rs.getString(13));
//                mainPlot.setHomeworkOpenCnt(rs.getInt(14));
//                mainPlot.setChallengeOpenCnt(rs.getInt(15));
//                mainPlot.setTotalOpenCnt(rs.getInt(16));
//
//
//                //相同班级放入同一集合
//                if (classMainPlots.containsKey(mainPlot.getClassId())){
//                    classMainPlots.get(mainPlot.getClassId()).add(mainPlot);
//                }else{//不同班纳放入新集合
//                    ArrayList<MainPlot> mainPlots = new ArrayList<>();
//                    mainPlots.add(mainPlot);
//                    classMainPlots.put(mainPlot.getClassId(),mainPlots);
//                }
//            }
//
//            //发送查询结果
//            ctx.collect(classMainPlots);
//            //mainPlots.clear();
//            classMainPlots.clear();
//            Thread.sleep(60000);
//        }
//    }
//
//
//    //关闭连接
//    @Override
//    public void cancel() {
//        try {
//            if(myConnect!=null){
//                myConnect.close();
//            }
//            if(homeworkCntPS!=null){
//                homeworkCntPS.close();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        isRunning=false;
//    }
//}