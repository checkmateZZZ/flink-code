package com.hetao101.SupervisionAnalyse.dao.reader;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

/**
 * @author wangcong
 * @date 2019/10/24
 */
//读取用户调班信息
public class UserClassReader extends RichSourceFunction<ArrayList<Tuple5<String, String, String, String, String>>> {
    private Connection myConnect = null;
    private PreparedStatement userClassPS = null;
    private Boolean isRunning;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning=true;
        Class.forName("com.mysql.jdbc.Driver");
        myConnect = DriverManager
                .getConnection("jdbc:mysql://datacenterdb.testing.pipacoding.com:3306/htbc_app?characterEncoding=utf-8&useSSL=true", "root", "tfeGNtOhoxTk6c1I");

        userClassPS = myConnect.prepareStatement("select distinct " +
                "user_id," +
                "class_id," +
                "status," +
                "start_time," +
                "end_time " +
                "from htbc_dw.dw_user_class " +
                "where class_type in (1,4,8,9)");

    }

    @Override
    public void run(SourceContext<ArrayList<Tuple5<String, String, String, String, String>>> ctx) throws Exception {

        ArrayList<Tuple5<String,String,String,String,String>> userClasses = new ArrayList<>();
        while (isRunning) {
            ResultSet rs = userClassPS.executeQuery();
            while (rs.next()){
                Tuple5<String, String, String, String, String> userClass = new Tuple5<>(rs.getString(1)
                        , rs.getString(2)
                        , rs.getString(3)
                        , rs.getString(4)
                        , rs.getString(5));

                userClasses.add(userClass);
            }
            ctx.collect(userClasses);
            userClasses.clear();
            Thread.sleep(100000);
        }
    }

    @Override
    public void cancel() {
        try{
            if (myConnect != null) {
                myConnect.close();
            }
            if (userClassPS != null) {
                userClassPS.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
