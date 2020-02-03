package com.hetao101.SupervisionAnalyse.spark.counselor;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Demo2 {
    private final Logger log = Logger.getLogger(Demo2.class);

    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .appName("Demo2")
                .master("local[2]")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

//        IDimClassDAO iClassDAO = DaoFactory.getIClassDAO();
//        ArrayList<DimClass> dimClass = iClassDAO.query();
//        JavaRDD<DimClass> dimclass = jsc.parallelize(dimClass);
//        dimClass.

        //SQLContext sqlContext = new SQLContext(jsc);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(jsc, Duration.apply(5000));

        JavaReceiverInputDStream<String> socketTextStream = javaStreamingContext.socketTextStream("10.100.1.156", 9999);

        JavaPairDStream<String, Long> stream =
                socketTextStream.flatMapToPair(new PairFlatMapFunction<String, String, Long>() {
            @Override
            public Iterator<Tuple2<String, Long>> call(String s) throws Exception {
                String[] words = s.split(" ");
                List<Tuple2<String, Long>> list = new ArrayList<>();

                for (String word : words) {
                    list.add(new Tuple2<String, Long>(word, 1L));
                }

                return list.listIterator();
            }
        });


        JavaPairDStream<String, Long> rs = stream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });

        rs.print();

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();








        /*
        读取mysql中的数据，并注册成临时表
         */
 //       Properties properties = new Properties();
//        properties.setProperty("user","root");
//        properties.setProperty("password","tfeGNtOhoxTk6c1I");
//        properties.setProperty("driver","com.mysql.jdbc.Driver");
//        String url = "jdbc:mysql://datacenterdb.testing.pipacoding.com";
//        //String table = "htbc_dw.dw_course_apply_order";
//        sqlContext.read().jdbc(url, "htbc_dw.dw_course_apply_order", properties).select("*")
//                .registerTempTable("dw_course_apply_order");
//        sqlContext.read().jdbc(url, "htbc_dw.dim_class", properties).select("*")
//                .registerTempTable("dim_class");
//
//
//        String sql="select o.term_id,count(distinct o.class_id) " +
//                "from dw_course_apply_order o " +
//                "left join dim_class c on o.class_id=c.class_id " +
//                "where c.term_id=39 " +
//                "group by o.term_id";
        //Dataset<Row> rs = sqlContext.sql(sql);


        //rs.show();


        //jsc.close();




    }
}
