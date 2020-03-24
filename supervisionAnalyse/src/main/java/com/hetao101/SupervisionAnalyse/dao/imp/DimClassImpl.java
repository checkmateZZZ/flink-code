package com.hetao101.SupervisionAnalyse.dao.imp;

import com.hetao101.SupervisionAnalyse.dao.IDimClassDAO;
import com.hetao101.SupervisionAnalyse.entity.DimClass;
import com.hetao101.SupervisionAnalyse.util.JdbcUtils;
import java.sql.ResultSet;
import java.util.ArrayList;

/**
 * 获取班级表信息
 * @author wangcong
 * @date 2019/9/26
 */
public class DimClassImpl implements IDimClassDAO {

    @Override
    public ArrayList<DimClass> query() {

        ArrayList<DimClass> dimClasses = new ArrayList<>();
        //DimClass dimclass = new DimClass();
        String sql = "select * from htbc_dw.dim_class";
        JdbcUtils jdbcUtil = JdbcUtils.getInstance();
        jdbcUtil.executeQuery(sql, new JdbcUtils.QueryCallback() {

            @Override
            public void process(ResultSet rs) throws Exception {

                while (rs.next()){
                    DimClass dimclass = new DimClass();

                    int classID = rs.getInt(1);
                    String className = rs.getString(2);
                    int counselorId = rs.getInt(3);
                    String counselorName = rs.getString(4);
                    int classType = rs.getInt(5);
                    int coursePackageId = rs.getInt(6);
                    String coursePackageName = rs.getString(7);
                    String courseType = rs.getString(8);
                    String grade = rs.getString(10);
                    String classOpenTime = rs.getString(11);
                    int termID = rs.getInt(12);
                    String termName = rs.getString(13);
                    int courseGroup = rs.getInt(19);
                    String status = rs.getString(22);
                    String loadTime = rs.getString(24);

//                    logger.info("班级ID:"+classID);

                    dimclass.setClassId(classID);
                    dimclass.setClassName(className);
                    dimclass.setCounselorId(counselorId);
                    dimclass.setCounselorName(counselorName);
                    dimclass.setClassType(classType);
                    dimclass.setCoursePackageId(coursePackageId);
                    dimclass.setcoursePackageName(coursePackageName);
                    dimclass.setCourseType(courseType);
                    dimclass.setGrade(grade);
                    dimclass.setClassOpenTime(classOpenTime);
                    dimclass.setTermID(termID);
                    dimclass.setTermName(termName);
                    dimclass.setCourseGroup(courseGroup);
                    dimclass.setStatus(status);
                    dimclass.setLoadTime(loadTime);

                    dimClasses.add(dimclass);
                }
            }

        });

        return dimClasses;
    }
}
