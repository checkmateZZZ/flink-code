package com.hetao101.SupervisionAnalyse.dao.Factory;

import com.hetao101.SupervisionAnalyse.dao.IClassCourseAllPlotDAO;
import com.hetao101.SupervisionAnalyse.dao.IDimClassDAO;
import com.hetao101.SupervisionAnalyse.dao.imp.DimClassImpl;
import com.hetao101.SupervisionAnalyse.dao.imp.IClassCourseAllPlotImpl;

/**
 * DAO层工厂类
 * @author wangcong
 * @date 2019/9/26
 */
public class DaoFactory {
    public static IDimClassDAO getIClassDAO(){
        return new DimClassImpl();
    }
    public static IClassCourseAllPlotDAO getIClassAllPlot(){
        return new IClassCourseAllPlotImpl();
    }
}
