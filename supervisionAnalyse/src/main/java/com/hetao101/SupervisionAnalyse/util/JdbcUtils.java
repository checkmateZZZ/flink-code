package com.hetao101.SupervisionAnalyse.util;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

/**
 * @author wangcong
 * @date 2019/9/25
 */
public class JdbcUtils {
    static {
        try {
            String driver = ConfigManager.getProperty(Constants.MYSQL_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static JdbcUtils instance = null;


    public static JdbcUtils getInstance() {
        if(instance == null) {
            synchronized(JdbcUtils.class) {
                if(instance == null) {
                    instance = new JdbcUtils();
                }
            }
        }
        return instance;
    }

    // 数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();


    private JdbcUtils() {
        // 首先第一步，获取数据库连接池的大小，就是说，数据库连接池中要放多少个数据库连接
        // 这个，可以通过在配置文件中配置的方式，来灵活的设定
        int datasourceSize = ConfigManager.getInteger(
                Constants.JDBC_DATASOURCE_SIZE);

        // 然后创建指定数量的数据库连接，并放入数据库连接池中
        for(int i = 0; i < datasourceSize; i++) {
            boolean local = ConfigManager.getBoolean("true");
            String url = null;
            String user = null;
            String password = null;

            if(local) {
                url = ConfigManager.getProperty(Constants.MYSQL_URL_TEST);
                user = ConfigManager.getProperty(Constants.MYSQL_USER_TEST);
                password = ConfigManager.getProperty(Constants.MYSQL_PASSWORD_TEST);
            } else {
//                url = ConfigManager.getProperty(Constants.JDBC_URL_PROD);
//                user = ConfigManager.getProperty(Constants.JDBC_USER_PROD);
//                password = ConfigManager.getProperty(Constants.JDBC_PASSWORD_PROD);
            }

            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public synchronized Connection getConnection() {
        while(datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }


    
    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            // 取消自动提交
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);

            if(params != null && params.length > 0) {
                for(int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            rtn = pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 执行查询SQL语句
     * @param sql
     * @param callback
     */
    public void executeQuery(String sql,
                             QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

//            if(params != null && params.length > 0) {
//                for(int i = 0; i < params.length; i++) {
//                    pstmt.setObject(i + 1, params[i]);
//                }
//            }

            rs = pstmt.executeQuery();

            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                datasource.push(conn);
            }
        }
    }



    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();

            // 第一步：使用Connection对象，取消自动提交
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);

            // 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
            if(paramsList != null && paramsList.size() > 0) {
                for(Object[] params : paramsList) {
                    for(int i = 0; i < params.length; i++) {
                        pstmt.setObject(i + 1, params[i]);
                    }
                    pstmt.addBatch();
                }
            }

            // 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstmt.executeBatch();

            // 第四步：使用Connection对象，提交批量的SQL语句
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 静态内部类：查询回调接口
     * @author wangcong
     *
     */
    public static interface QueryCallback {

        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;

    }
}
