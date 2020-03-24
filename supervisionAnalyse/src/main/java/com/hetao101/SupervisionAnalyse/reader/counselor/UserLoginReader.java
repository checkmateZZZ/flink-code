package com.hetao101.SupervisionAnalyse.reader.counselor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

import static com.hetao101.SupervisionAnalyse.util.TimeUtil.parseLong2StringNew;

/**
 * @author wangcong
 * @date 2019/10/22
 */
//读取用户登陆表
public class UserLoginReader extends RichSourceFunction<ArrayList<Integer>> {
    private Connection myConnect = null;
    private PreparedStatement userForbidenVisitPS = null;
    private ResultSet rs = null;
    private String CurrentParDate = parseLong2StringNew(System.currentTimeMillis(),"yyyy-MM-dd");
    private Boolean isRunning;



    //初始化 建立读取连接
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning=true;
        Class.forName(ConfigManager.getProperty(Constants.MYSQL_DRIVER));
        Properties mysqlProp = new Properties();
        mysqlProp.setProperty("user", ConfigManager.getProperty(Constants.ONLINE_LOGIC_USER));
        mysqlProp.setProperty("password",ConfigManager.getProperty(Constants.ONLINE_LOGIC_PASSWORD));

        myConnect = DriverManager
                .getConnection(ConfigManager.getProperty(Constants.ONLINE_LOGIC_URL),mysqlProp);

        //0已回访 1停止回访
        userForbidenVisitPS = myConnect.prepareStatement(
                "select distinct user_id " +
                "from channel_admin.channel_data " +
                        "where utime>=? and (JSON_EXTRACT(COMMENT, '$.loginWay')='account_and_password' OR JSON_EXTRACT(COMMENT, '$.loginWay')='clientScanLogin');");
    }

    //将读到的结果输入数据流
    @Override
    public void run(SourceContext<ArrayList<Integer>> ctx) throws Exception {

        //ArrayList<classId,userId,unitId,status>
        ArrayList<Integer> userLogins = new ArrayList<>();
        while (isRunning) {
            userForbidenVisitPS.setString(1,CurrentParDate);
            rs = userForbidenVisitPS.executeQuery();
            while (rs.next()) {
                userLogins.add(rs.getInt(1));
            }
            //发送查询结果
            ctx.collect(userLogins);
            userLogins.clear();
            Thread.sleep(120000);
        }
    }

    //关闭连接
    @Override
    public void cancel() {
        try {
            if (rs != null) {
                rs.close();
            }
            if (userForbidenVisitPS != null) {
                userForbidenVisitPS.close();
            }
            if (myConnect != null) {
                myConnect.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
