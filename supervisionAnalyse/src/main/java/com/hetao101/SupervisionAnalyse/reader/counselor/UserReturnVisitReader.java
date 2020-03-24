package com.hetao101.SupervisionAnalyse.reader.counselor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

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
//读取回访表
public class UserReturnVisitReader extends RichSourceFunction<ArrayList<Row>> {
    private Connection myConnect = null;
    private PreparedStatement UserReturnVisitPS = null;
    private String CurrentParDate = parseLong2StringNew(System.currentTimeMillis(),"yyyy-MM-dd");
    private Boolean isRunning;



    //初始化 建立读取连接
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning=true;
        Class.forName(ConfigManager.getProperty(Constants.MYSQL_DRIVER));
        Properties mysqlProp = new Properties();
        mysqlProp.setProperty("user", ConfigManager.getProperty(Constants.ONLINE_CRM_USER));
        mysqlProp.setProperty("password",ConfigManager.getProperty(Constants.ONLINE_CRM_PASSWORD));

        myConnect = DriverManager
                .getConnection(ConfigManager.getProperty(Constants.ONLINE_CRM_URL),mysqlProp);

        //0已回访 1停止回访
        UserReturnVisitPS = myConnect.prepareStatement(
                "select distinct class_id,user_id,unit_id " +
                "from crm.return_visit where utime>=?;");
    }

    //将读到的结果输入数据流
    @Override
    public void run(SourceContext<ArrayList<Row>> ctx) throws Exception {

        //ArrayList<classId,userId,unitId,status>
        ArrayList<Row> userReturnVisits = new ArrayList<>();
        while (isRunning) {
            UserReturnVisitPS.setString(1,CurrentParDate);
            ResultSet rs = UserReturnVisitPS.executeQuery();
            while (rs.next()) {
                Row row = new Row(3);
                row.setField(0,rs.getInt(1));
                row.setField(1,rs.getInt(2));
                row.setField(2,rs.getInt(3));
                userReturnVisits.add(row);
            }
            //发送查询结果
            ctx.collect(userReturnVisits);
            userReturnVisits.clear();
            Thread.sleep(120000);
        }
    }

    //关闭连接
    @Override
    public void cancel() {
        try {
            if (UserReturnVisitPS != null) {
                UserReturnVisitPS.close();
            }
            if (myConnect != null) {
                myConnect.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
