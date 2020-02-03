package com.hetao101.SupervisionAnalyse.common;

/**
 * @author wangcong
 * @date 2019/10/14
 */
public enum DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public String dateType;

    DateEnum() {
    }

    DateEnum(String dateType) {
        this.dateType = dateType;
    }

    public static DateEnum valueOfType(String type){
        for (DateEnum dateEnum : values()) {
            if(dateEnum.dateType.equals(type)){
                return dateEnum;
            }
        }
        return null;
    }
}
