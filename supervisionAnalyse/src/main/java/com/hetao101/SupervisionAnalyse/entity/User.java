package com.hetao101.SupervisionAnalyse.entity;

/**
 * 用户信息表 ods_main_user
 * @author wangcong
 * @date 2019/9/25
 */
public class User {
    private int id;
    private int openID;
    private String nickName;
    private String wxName;
    private long phoneNumber;
    private int gender;
    private String province;
    private String city;
    private int age;
    private int add_wx;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getOpenID() {
        return openID;
    }

    public void setOpenID(int openID) {
        this.openID = openID;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public String getWxName() {
        return wxName;
    }

    public void setWxName(String wxName) {
        this.wxName = wxName;
    }

    public long getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(long phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getAdd_wx() {
        return add_wx;
    }

    public void setAdd_wx(int add_wx) {
        this.add_wx = add_wx;
    }
}
