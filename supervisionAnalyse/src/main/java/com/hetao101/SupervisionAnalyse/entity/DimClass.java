package com.hetao101.SupervisionAnalyse.entity;

/**
 * 班级维表 dim_class
 * @author wangcong
 * @date 2019/9/25
 */
public class DimClass {
    private int classId;
    private String className;
    private int counselorId;
    private String counselorName;
    private int classType;
    private int coursePackageId;
    private String coursePackageName;
    private String courseType;
    private String grade;
    private String classOpenTime;
    private int termID;
    private String termName;
    private int courseGroup;
    private String status;
    private String loadTime;

    public int getClassId() {
        return classId;
    }

    public void setClassId(int classId) {
        this.classId = classId;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public int getCounselorId() {
        return counselorId;
    }

    public void setCounselorId(int counselorId) {
        this.counselorId = counselorId;
    }

    public String getCounselorName() {
        return counselorName;
    }

    public void setCounselorName(String counselorName) {
        this.counselorName = counselorName;
    }

    public int getClassType() {
        return classType;
    }

    public void setClassType(int classType) {
        this.classType = classType;
    }

    public int getCoursePackageId() {
        return coursePackageId;
    }

    public void setCoursePackageId(int coursePackageId) {
        this.coursePackageId = coursePackageId;
    }

    public String getcoursePackageName() {
        return coursePackageName;
    }

    public void setcoursePackageName(String coursePackageName) {
        this.coursePackageName = coursePackageName;
    }

    public String getCourseType() {
        return courseType;
    }

    public void setCourseType(String courseType) {
        this.courseType = courseType;
    }

    public String getGrade() {
        return grade;
    }

    public void setGrade(String grade) {
        this.grade = grade;
    }

    public String getClassOpenTime() {
        return classOpenTime;
    }

    public void setClassOpenTime(String classOpenTime) {
        this.classOpenTime = classOpenTime;
    }

    public int getTermID() {
        return termID;
    }

    public void setTermID(int termID) {
        this.termID = termID;
    }

    public String getTermName() {
        return termName;
    }

    public void setTermName(String termName) {
        this.termName = termName;
    }

    public int getCourseGroup() {
        return courseGroup;
    }

    public void setCourseGroup(int courseGroup) {
        this.courseGroup = courseGroup;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getLoadTime() {
        return loadTime;
    }

    public void setLoadTime(String loadTime) {
        this.loadTime = loadTime;
    }
}
