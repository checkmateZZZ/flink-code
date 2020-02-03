package com.hetao101.SupervisionAnalyse.entity;

/**
 * @author wangcong
 * @date 2019/10/25
 */
public class UserDemo {
    int userId;

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "UserDemo{" +
                "userId=" + userId +
                '}';
    }
}
