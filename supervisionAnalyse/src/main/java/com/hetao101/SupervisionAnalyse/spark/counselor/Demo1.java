package com.hetao101.SupervisionAnalyse.spark.counselor;

import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author wangcong
 * @date 2019/10/3
 */
public class Demo1 {
    public static void main(String[] args) throws InterruptedException {

        //上下文入口
        SparkSession kafkaComsumer = SparkSession
                .builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .appName("kafkaComsumer")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(kafkaComsumer.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(javaSparkContext, Duration.apply(60000));


        //kafka参数配置
        Map<String, Object> kafkaParam = new HashMap<String, Object>();
        kafkaParam.put("bootstrap.servers", ConfigManager.getProperty(Constants.BOOTSTRAP_SEVERS_1));
        kafkaParam.put("group.id","realTime");
        kafkaParam.put("auto.offset.reset","latest");
        kafkaParam.put("key.deserializer", StringDeserializer.class);
        kafkaParam.put("value.deserializer", StringDeserializer.class);
        kafkaParam.put("enable.auto.commit", false);

        //遍历并拿到所有topic名称
        String kafkaTopics = ConfigManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<String>();
        for (String kafkaTopic : kafkaTopicSplited) {
            topics.add(kafkaTopic);
        }

        //Direct方式拿到kafka流数据
        JavaInputDStream<ConsumerRecord<String, String>> DStream =
                KafkaUtils.createDirectStream(jssc
                        , LocationStrategies.PreferConsistent()
                        , ConsumerStrategies.Subscribe(topics,kafkaParam));

        DStream.print();
        //区分kafka数据模块 <topic,data>
        JavaPairDStream<String, String> dataItems = labelDataItems(DStream);
        //dataItems.print();

        //统计各学员、各unit作业完成情况 以及挑战进度情况  生成(userId_classId_unitID)
        JavaPairDStream<String, String> scriptHomeworkStat = dataItems.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> di) throws Exception {
                String topic = di._1;
                if (topic.equals("hetao-main-order")) {
                    return true;
                }
                return false;
            }
        }).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> scriptHomework) throws Exception {
                String topic = scriptHomework._1;
                String data = scriptHomework._2;
//                JSONObject objects = JSON.parseObject(data);
//
//                String userId = objects.getString("userId");
//                JSONObject courseInfo = objects.getJSONObject("course");
//                String courseName = courseInfo.getString("name");
//
//                String rs = userId + "_" + courseName;

                return new Tuple2<String, String>(topic, data);
            }
        });

        scriptHomeworkStat.print();


        jssc.start();
        jssc.awaitTermination();

    }














    //区分kafka数据模块 <topic,data>
    private static JavaPairDStream<String, String> labelDataItems(JavaInputDStream DStream) {
        JavaPairDStream<String, String> dataItems = DStream.mapToPair(new PairFunction<ConsumerRecord<String, String >, String, String>() {
            @Override
            public Tuple2<String, String> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                String item = consumerRecord.topic();
                String data = consumerRecord.value();

                return new Tuple2<String, String>(item,data);
            }
        });
        return dataItems;
    }
}
