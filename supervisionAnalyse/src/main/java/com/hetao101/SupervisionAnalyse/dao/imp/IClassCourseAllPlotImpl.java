package com.hetao101.SupervisionAnalyse.dao.imp;

import com.hetao101.SupervisionAnalyse.dao.IClassCourseAllPlotDAO;
import com.hetao101.SupervisionAnalyse.entity.ClassCourseAllPlot;
import com.hetao101.SupervisionAnalyse.util.JdbcUtils;

import java.sql.ResultSet;
import java.util.ArrayList;

/**
 * @author wangcong
 * @date 2019/10/14
 */
public class IClassCourseAllPlotImpl implements IClassCourseAllPlotDAO {
    @Override
    public ArrayList<ClassCourseAllPlot> query() {
        
        ArrayList<ClassCourseAllPlot> classCourseAllPlots = new ArrayList<>();
        String sql = "select " +
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
                ",unlocked" +
                ",unit_unlocked_time" +
                ",chapter_id" +
                ",chapter_name" +
                ",item_type" +
                ",tag" +
                ",finish_tag" +
                "from htbc_dw.dw_class_course_all_plot" +
                "where class_type in (4,8,9)";
        JdbcUtils jdbcUtil = JdbcUtils.getInstance();
        jdbcUtil.executeQuery(sql, new JdbcUtils.QueryCallback() {

            @Override
            public void process(ResultSet rs) throws Exception {

                while (rs.next()){
                    ClassCourseAllPlot classCourseAllPlot = new ClassCourseAllPlot();



//                    classCourseAllPlot.setClassId(classID);
//                    classCourseAllPlot.setClassName(className);
//                    classCourseAllPlot.setCounselorId(counselorId);
//                    classCourseAllPlot.setCounselorName(counselorName);
//                    classCourseAllPlot.setClassType(classType);
//                    classCourseAllPlot.setCoursePackageId(coursePackageId);
//                    classCourseAllPlot.setcoursePackageName(coursePackageName);
//                    classCourseAllPlot.setCourseType(courseType);
//                    classCourseAllPlot.setGrade(grade);
//                    classCourseAllPlot.setClassOpenTime(classOpenTime);
//                    classCourseAllPlot.setTermID(termID);
//                    classCourseAllPlot.setTermName(termName);
//                    classCourseAllPlot.setCourseGroup(courseGroup);
//                    classCourseAllPlot.setStatus(status);
//                    classCourseAllPlot.setLoadTime(loadTime);

                    classCourseAllPlots.add(classCourseAllPlot);
                }
            }

        });

        return classCourseAllPlots;
    }
}
