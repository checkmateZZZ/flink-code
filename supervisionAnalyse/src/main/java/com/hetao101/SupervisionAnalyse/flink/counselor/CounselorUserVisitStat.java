//package com.hetao101.SupervisionAnalyse.flink.counselor;
//
//import com.hetao101.SupervisionAnalyse.common.Constants;
//import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
//
//import java.util.Properties;
//
//public class CounselorUserVisitStat {
//    public static void main(String[] args) throws Exception {
////        System.out.println(parseLong2StringNew(System.currentTimeMillis(),"yyyy-MM-dd"));
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        //kafka配置信息
//        Properties kafkaProp = new Properties();
//        kafkaProp.setProperty("bootstrap.servers", ConfigManager.getProperty(Constants.BOOTSTRAP_SEVERS_1));
//        kafkaProp.setProperty("group.id", Constants.GROUP_ID);
//
//        //提交测试题流
//        DataStreamSource<String> examAnswerStream =
//                env.addSource(new FlinkKafkaConsumer010("hetao-submit-examAnswer", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1582624800000")));
//        examAnswerStream.print();
//
//        env.execute();
//
//    }
//}
