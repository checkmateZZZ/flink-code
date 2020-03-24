//package com.hetao101.SupervisionAnalyse.flink.ReportPush;
//
//import com.alibaba.dts.formats.avro.Record;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.hetao101.SupervisionAnalyse.common.Constants;
//import com.hetao101.SupervisionAnalyse.reader.ApplyReportPushReader;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple6;
//import org.apache.flink.formats.avro.AvroDeserializationSchema;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
//import org.apache.flink.util.Collector;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Properties;
//
///**
// * @author wangcong
// * @date 2019/10/23
// */
////企业微信续报数据推送
//public class ApplyReportPush {
//    //private static final Logger log = Logger.getLogger(ApplyReportPush.class);
//    private static final Logger logger = LoggerFactory.getLogger(ApplyReportPush.class);
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
////        env.enableCheckpointing(1000);
////        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
////        env.setStateBackend(new FsStateBackend("hdfs:///flink/flink-checkpoints",true));
////        //失败重启策略
////        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
////                5, //重启次数
////                Time.of(10, TimeUnit.SECONDS) //间隔
////        ));
//        //StreamTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);
//
//
//        //kafka配置信息
//        Properties kafkaProp = new Properties();
//        kafkaProp.setProperty("bootstrap.servers","10.100.3.133:9092,10.100.3.132:9092,10.100.3.134:9092");
//        kafkaProp.setProperty("group.id", Constants.GROUP_ID);
//
//        //用户调班广播描述器
//        MapStateDescriptor<String, ArrayList<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>> ucIBroadCastDesc =
//                new MapStateDescriptor("ucIBroadCastStream"
//                        , BasicTypeInfo.STRING_TYPE_INFO
//                        , TypeInformation.of(new TypeHint<ArrayList<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>>() {
//                }));
//        //用户调班
//        BroadcastStream<ArrayList<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>> ucIBroadCastStream =
//                env.addSource(new ApplyReportPushReader()).broadcast(ucIBroadCastDesc);
//
//
//        //获取订单流
//        DataStreamSource<GenericRecord> orderStream =
//                env.addSource(new FlinkKafkaConsumer010("hetao-main-order", AvroDeserializationSchema.forGeneric(Record.getClassSchema()),kafkaProp).setStartFromLatest());
//        //orderStream.print();
//
//
//
//        applyStat(orderStream,ucIBroadCastStream);
//
//
//
//        env.execute();
//        //tenv.execEnv();
//    }
//
//
//
//
//    //续报人数统计
//    private static void applyStat(DataStreamSource<GenericRecord> orderStream
//            ,BroadcastStream<ArrayList<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>> ucIBroadCastStream) {
//
//        SingleOutputStreamOperator<Tuple3<Integer,Integer,Integer>> resultStream = orderStream.filter(new FilterFunction<GenericRecord>() {
//            @Override
//            public boolean filter(GenericRecord value) throws Exception {
//
//                if(value!=null && value.get(7)!=null && value.get(12)!=null && value.get(13)!=null){
//
//                    String operation = value.get(7).toString();
//                    String beforeStatus = JSONObject.parseArray(value.get(12).toString()).getJSONObject(14).getString("value");
//                    String afterStatus = JSONObject.parseArray(value.get(13).toString()).getJSONObject(14).getString("value");
//                    String courseType = JSONObject.parseArray(value.get(13).toString()).getJSONObject(30).getString("value");
//
//                    if (operation.equals("INSERT") && afterStatus.equals("PAID") && !courseType.equals("L1")
//                        || operation.equals("UPDATE") && !beforeStatus.equals("PAID") && afterStatus.equals("PAID") && !courseType.equals("L1")) {
//                    return true;
//                } else {
//                    return false;
//                }
//                }
//                return false;
//            }
//        }).map(new MapFunction<GenericRecord, Tuple2<JSONArray, JSONArray>>() {
//            @Override
//            public Tuple2<JSONArray, JSONArray> map(GenericRecord value) throws Exception {
//                //logger.info("进入map~");
//                JSONArray beforeImages = JSONObject.parseObject(String.valueOf(value)).getJSONArray("beforeImages");
//                JSONArray afterImages = JSONObject.parseObject(String.valueOf(value)).getJSONArray("afterImages");
//
//                String userId = JSONObject.parseObject(String.valueOf(value)).getJSONArray("afterImages").getJSONObject(5).getString("value");
//                String marketChannel = JSONObject.parseObject(String.valueOf(value)).getJSONArray("afterImages").getJSONObject(12).getString("value");
//                String orderNumber = JSONObject.parseArray(value.get(13).toString()).getJSONObject(3).getString("value");
//                String payTime = JSONObject.parseArray(value.get(13).toString()).getJSONObject(25).getString("value");
//                //logger.info("order流：user_id："+userId+" class_id："+marketChannel+" order_number："+orderNumber+"pay_time："+payTime);
//
//                return new Tuple2<>(beforeImages, afterImages);
//            }
//        }).connect(ucIBroadCastStream)
//                .process(new BroadcastProcessFunction<Tuple2<JSONArray, JSONArray>, ArrayList<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>, Tuple3<Integer,Integer,Integer>>() {
//                    MapStateDescriptor<String,Tuple2<HashMap<Integer, Tuple2<Integer,Integer>>,HashMap<Integer, Tuple2<Integer,Integer>>>> ucIBroadCastDesc =
//                            new MapStateDescriptor("ucIBroadCastStream"
//                                    , BasicTypeInfo.STRING_TYPE_INFO
//                                    , TypeInformation.of(new TypeHint<Tuple2<HashMap<Integer, Tuple2<Integer,Integer>>,HashMap<Integer, Tuple2<Integer,Integer>>>>() {
//                            }));
//                    @Override
//                    public void processElement(Tuple2<JSONArray, JSONArray> value, ReadOnlyContext ctx, Collector<Tuple3<Integer,Integer,Integer>> out) throws Exception {
//                        //拿到用户调班维表
//                        //Thread.sleep(20000);
//                        Tuple2<HashMap<Integer, Tuple2<Integer, Integer>>, HashMap<Integer, Tuple2<Integer, Integer>>> broadCastStatevalue =
//                                ctx.getBroadcastState(ucIBroadCastDesc).get("BroadCastStateKey");
//
//                        if (broadCastStatevalue!=null) {
//                            logger.info("开始逻辑处理~");
//                            HashMap<Integer, Tuple2<Integer, Integer>> userTermCounselor = broadCastStatevalue.f0;
//                            HashMap<Integer, Tuple2<Integer, Integer>> classTermcounselor = broadCastStatevalue.f1;
//                            JSONArray afterImages = value.f1;
//                            Integer userId = afterImages.getJSONObject(5).getInteger("value");
//                            logger.info("145行：" + userId);
//                            Integer marketChannel = JSONObject.parseObject(afterImages.getJSONObject(12).getString("value")).getInteger("bytes");
//                            //Row row = new Row(3);
//                            //如果当前订单userId存在L1班级,则返回所在L1班级id
//                            if (userTermCounselor.containsKey(userId)) {
////                                row.setField(0, userTermCounselor.get(userId).f0);
////                                row.setField(1, userTermCounselor.get(userId).f1);
////                                row.setField(2, 1);
//                                out.collect(new Tuple3(userTermCounselor.get(userId).f0,userTermCounselor.get(userId).f1,1));
//                            } else {//不存在L1班级,则使用订单班级
//                                try {
////                                    row.setField(0, classTermcounselor.get(marketChannel).f0);
////                                    row.setField(1, classTermcounselor.get(marketChannel).f1);
////                                    row.setField(2, 1);
//                                    out.collect(new Tuple3(userTermCounselor.get(marketChannel).f0,userTermCounselor.get(marketChannel).f1,1));
//                                }catch (Exception e){
//                                    logger.info("classId=0 or 年课班级："+marketChannel);
//                                }
//                            }
//                        } else {
//                            logger.info("未连接到数据库！");
//                        }
//                    }
//
//                    @Override
//                    public void processBroadcastElement(ArrayList<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> value, Context ctx, Collector<Tuple3<Integer,Integer,Integer>> out) throws Exception {
//                        logger.info("广播数据是否为空：" + value.isEmpty());
//
//                        //<userId,<termId,counselorId>>
//                        HashMap<Integer, Tuple2> userTermCounselor = new HashMap<>();
//                        //<classId,<termId,counselorId>>
//                        HashMap<Integer, Tuple2> classTermcounselor = new HashMap<>();
//                        for (Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> userClassInfo : value) {
//
//                            if (userClassInfo.f5.equals(1)) {
//                                userTermCounselor.put(userClassInfo.f1,new Tuple2(userClassInfo.f0,userClassInfo.f2) );
//                            }
//                            classTermcounselor.put(userClassInfo.f3,new Tuple2(userClassInfo.f0,userClassInfo.f2) );
//                        }
//
//                        ctx.getBroadcastState(ucIBroadCastDesc).put("BroadCastStateKey", new Tuple2(userTermCounselor,classTermcounselor));
//                }
//                });
//
//        resultStream.print();
//    }
//}
//
//
//
//
