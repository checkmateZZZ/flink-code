package com.hetao101.SupervisionAnalyse.entity;

/**
 * @author wangcong
 * @date 2019/10/15
 */

//输出 L1督课督学 主表部分
public class MainPlotUser {
    Integer userId;
    Integer classId;
    String className;
    Integer grade;
    Integer classType;
    Integer termId;
    String termName;
    Integer counselorId;
    String counselorName;
    Integer courseLevel;
    Integer unitId;
    String unitName;
    Integer unitSequence;
    String unitUnlockedTime;
    Integer homeworkOpenCnt;
    Integer challengeOpenCnt;
    Integer totalOpenCnt;

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getClassId() {
        return classId;
    }

    public void setClassId(Integer classId) {
        this.classId = classId;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Integer getGrade() {
        return grade;
    }

    public void setGrade(Integer grade) {
        this.grade = grade;
    }

    public Integer getClassType() {
        return classType;
    }

    public void setClassType(Integer classType) {
        this.classType = classType;
    }

    public Integer getTermId() {
        return termId;
    }

    public void setTermId(Integer termId) {
        this.termId = termId;
    }

    public String getTermName() {
        return termName;
    }

    public void setTermName(String termName) {
        this.termName = termName;
    }

    public Integer getCounselorId() {
        return counselorId;
    }

    public void setCounselorId(Integer counselorId) {
        this.counselorId = counselorId;
    }

    public String getCounselorName() {
        return counselorName;
    }

    public void setCounselorName(String counselorName) {
        this.counselorName = counselorName;
    }

    public Integer getCourseLevel() {
        return courseLevel;
    }

    public void setCourseLevel(Integer courseLevel) {
        this.courseLevel = courseLevel;
    }

    public Integer getUnitId() {
        return unitId;
    }

    public void setUnitId(Integer unitId) {
        this.unitId = unitId;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public Integer getUnitSequence() {
        return unitSequence;
    }

    public void setUnitSequence(Integer unitSequence) {
        this.unitSequence = unitSequence;
    }

    public String getUnitUnlockedTime() {
        return unitUnlockedTime;
    }

    public void setUnitUnlockedTime(String unitUnlockedTime) {
        this.unitUnlockedTime = unitUnlockedTime;
    }

    public Integer getHomeworkOpenCnt() {
        return homeworkOpenCnt;
    }

    public void setHomeworkOpenCnt(Integer homeworkOpenCnt) {
        this.homeworkOpenCnt = homeworkOpenCnt;
    }

    public Integer getChallengeOpenCnt() {
        return challengeOpenCnt;
    }

    public void setChallengeOpenCnt(Integer challengeOpenCnt) {
        this.challengeOpenCnt = challengeOpenCnt;
    }

    public Integer getTotalOpenCnt() {
        return totalOpenCnt;
    }

    public void setTotalOpenCnt(Integer totalOpenCnt) {
        this.totalOpenCnt = totalOpenCnt;
    }

    @Override
    public String toString() {
        return "MainPlotUser{" +
                "userId='" + userId + '\'' +
                ", classId='" + classId + '\'' +
                ", className='" + className + '\'' +
                ", grade='" + grade + '\'' +
                ", classType='" + classType + '\'' +
                ", termId='" + termId + '\'' +
                ", termName='" + termName + '\'' +
                ", counselorId='" + counselorId + '\'' +
                ", counselorName='" + counselorName + '\'' +
                ", courseId='" + courseLevel + '\'' +
                ", unitId='" + unitId + '\'' +
                ", unitName='" + unitName + '\'' +
                ", unitSequence='" + unitSequence + '\'' +
                ", unitUnlockedTime='" + unitUnlockedTime + '\'' +
                ", homeworkOpenCnt=" + homeworkOpenCnt +
                ", challengeOpenCnt=" + challengeOpenCnt +
                ", totalOpenCnt=" + totalOpenCnt +
                '}';
    }
}
