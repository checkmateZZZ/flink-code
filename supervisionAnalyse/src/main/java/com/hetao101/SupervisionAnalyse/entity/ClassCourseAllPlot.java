package com.hetao101.SupervisionAnalyse.entity;

/**
 * 剧情课树表 dw_class_course_all_plot
 * @author wangcong
 * @date 2019/9/25
 */
public class ClassCourseAllPlot {
    private Integer termId;
    private String termName;
    private Integer counselorId;
    private String counselorName;
    private Integer classId;
    private String className;
    private Integer grade;
    private String classOpenTime;
    private Integer classType;
    private String courseType;
    private Integer courseId;
    private Integer courseLevel;
    private Integer unitId;
    private String unitName;
    private Integer unitSequence;
    private Integer unlocked;
    private String unitUnlockedTime;
    private Integer chapterId;
    private String chapterName;
    private String itemType;
    private Integer tag;
    private Integer finishTag;

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

    public String getClassOpenTime() {
        return classOpenTime;
    }

    public void setClassOpenTime(String classOpenTime) {
        this.classOpenTime = classOpenTime;
    }

    public Integer getClassType() {
        return classType;
    }

    public void setClassType(Integer classType) {
        this.classType = classType;
    }

    public String getCourseType() {
        return courseType;
    }

    public void setCourseType(String courseType) {
        this.courseType = courseType;
    }

    public Integer getCourseId() {
        return courseId;
    }

    public void setCourseId(Integer courseId) {
        this.courseId = courseId;
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

    public Integer getUnlocked() {
        return unlocked;
    }

    public void setUnlocked(Integer unlocked) {
        this.unlocked = unlocked;
    }

    public String getUnitUnlockedTime() {
        return unitUnlockedTime;
    }

    public void setUnitUnlockedTime(String unitUnlockedTime) {
        this.unitUnlockedTime = unitUnlockedTime;
    }

    public Integer getChapterId() {
        return chapterId;
    }

    public void setChapterId(Integer chapterId) {
        this.chapterId = chapterId;
    }

    public String getChapterName() {
        return chapterName;
    }

    public void setChapterName(String chapterName) {
        this.chapterName = chapterName;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public Integer getTag() {
        return tag;
    }

    public void setTag(Integer tag) {
        this.tag = tag;
    }

    public Integer getFinishTag() {
        return finishTag;
    }

    public void setFinishTag(Integer finishTag) {
        this.finishTag = finishTag;
    }

    @Override
    public String toString() {
        return "ClassCourseAllPlot{" +
                "termId=" + termId +
                ", termName='" + termName + '\'' +
                ", counselorId=" + counselorId +
                ", counselorName='" + counselorName + '\'' +
                ", classId=" + classId +
                ", className='" + className + '\'' +
                ", grade=" + grade +
                ", classOpenTime='" + classOpenTime + '\'' +
                ", classType=" + classType +
                ", courseType=" + courseType +
                ", courseId=" + courseId +
                ", courseLevel=" + courseLevel +
                ", unitId=" + unitId +
                ", unitName='" + unitName + '\'' +
                ", unitSequence=" + unitSequence +
                ", unlocked=" + unlocked +
                ", unitUnlockedTime='" + unitUnlockedTime + '\'' +
                ", chapterId=" + chapterId +
                ", chapterName='" + chapterName + '\'' +
                ", itemType='" + itemType + '\'' +
                ", tag=" + tag +
                ", finishTag=" + finishTag +
                '}';
    }
}
