//package com.hetao101.SupervisionAnalyse.flink.counselor;
//
//import com.alibaba.fastjson.JSON;
//import com.hetao101.SupervisionAnalyse.common.Constants;
//import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
//import com.hetao101.SupervisionAnalyse.entity.ClassCourseAllPlot;
//import com.hetao101.SupervisionAnalyse.entity.MainPlot;
//import com.hetao101.SupervisionAnalyse.reader.counselor.*;
//import com.hetao101.SupervisionAnalyse.writer.counselor.*;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.Collector;
//import org.apache.log4j.Logger;
//
//import java.io.Serializable;
//import java.util.*;
//
//import static com.hetao101.SupervisionAnalyse.util.TimeUtil.parseLong2StringNew;
//
///**
// * @author wangcong
// * @date 2019/10/12
// */
//public class CounselorUserLearningStat implements Serializable {
//    private static Logger logger = Logger.getLogger(CounselorUserLearningStat.class);
//    public static ArrayList<Row> progressCache = new ArrayList();
//    public static ArrayList<Row> progressCache2 = new ArrayList();
//    public static ArrayList<Row> homeworkCache = new ArrayList();
//    public static ArrayList<Row> correctCache = new ArrayList();
//
//    public static void main(String[] args) throws Exception {
//
//        //创建运行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
////        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
////        env.setStateBackend(new FsStateBackend("hdfs://10.100.7.251:8020/flink/test",true));
////        //失败重启策略
////        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
////                5, //重启次数
////                Time.of(10, TimeUnit.SECONDS) //间隔
////        ));
////        env.setParallelism(4);
//
//
//        //kafka配置信息
//        Properties kafkaProp = new Properties();
//        kafkaProp.setProperty("bootstrap.servers", ConfigManager.getProperty(Constants.BOOTSTRAP_SEVERS_1));
//        kafkaProp.setProperty("group.id", Constants.GROUP_ID);
//
//        Properties ucKafkaProp = new Properties();
//        ucKafkaProp.setProperty("bootstrap.servers", ConfigManager.getProperty(Constants.BOOTSTRAP_SEVERS_2));
//        ucKafkaProp.setProperty("group.id", Constants.GROUP_ID);
//
//
//        //主表部分广播描述器
//        MapStateDescriptor<String, HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastDesc =
//                new MapStateDescriptor("UnlockCntBroadCastStream"
//                        , BasicTypeInfo.STRING_TYPE_INFO
//                        , TypeInformation.of(new TypeHint<HashMap<Integer, ArrayList<MainPlot>>>() {
//                }));
//        //将班课信息生成广播流
//        BroadcastStream<HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastStream =
//                env.addSource(new UnlockCntReaderHive()).broadcast(unlockCntBroadCastDesc);
//
//        //课程树广播描述器
//        MapStateDescriptor<String, ArrayList<ClassCourseAllPlot>> courseBroadCastDesc =
//                new MapStateDescriptor("CourseBroadCastStream"
//                        , BasicTypeInfo.STRING_TYPE_INFO
//                        , TypeInformation.of(new TypeHint<ArrayList<ClassCourseAllPlot>>() {
//                }));
//        BroadcastStream<ArrayList<ClassCourseAllPlot>> courseBroadCastStream =
//                env.addSource(new ClassCourseAllPlotReaderHive()).broadcast(courseBroadCastDesc);
//
//        //回访信息
//        DataStreamSource<ArrayList<Row>> userReturnVisitStream =
//                env.addSource(new UserReturnVisitReader());
//
//        //停止回访信息
//        DataStreamSource<ArrayList<Row>> userForbidenVisitStream =
//                env.addSource(new UserForbidenVisitReader());
//
//        //用户登陆信息
//        DataStreamSource<ArrayList<Integer>> userLoginStream =
//                env.addSource(new UserLoginReader());
//
//        //获取课程进度流
//        DataStreamSource<String> progressStream =
//                env.addSource(new FlinkKafkaConsumer010("hetao-learning-progress", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1582624800000")));
//
//        //获取作业流
//        DataStreamSource<String> homeworkStream =
//                env.addSource(new FlinkKafkaConsumer010("hetao-submit-homework", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1582624800000")));
//
//        //获取到课流
////        DataStreamSource<String> attendanceStream =
////                env.addSource(new FlinkKafkaConsumer010("hetao-attendance", new SimpleStringSchema(), kafkaProp).setStartFromLatest());
//
//        //批改作业流
//        DataStreamSource<String> correctStream =
//                env.addSource(new FlinkKafkaConsumer010("hetao-correct-homework", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1582624800000")));
//
//        //提交测试题流
//        DataStreamSource<String> examAnswerStream =
//                env.addSource(new FlinkKafkaConsumer010("hetao-submit-examAnswer", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1582624800000")));
//        examAnswerStream.print();
//
//
//        //进度、到课逻辑
//        //progressStruct(progressStream, courseBroadCastStream, courseBroadCastDesc, unlockCntBroadCastStream, unlockCntBroadCastDesc);
//
//        //完课逻辑
//        //homeworkStruct(homeworkStream,courseBroadCastStream,courseBroadCastDesc);
//
//        //点评作业逻辑
//        //correctStruct(correctStream,courseBroadCastStream,courseBroadCastDesc);
//
//        //回访逻辑
//        //returnVisitStruct(userReturnVisitStream);
//
//        //停止回访逻辑
//        //forbidenVisitStruct(userForbidenVisitStream);
//
//        //用户登陆逻辑
//        //userLoginStruct(userLoginStream);
//
//        //开始执行
//        env.execute();
//
//    }
//
//
//
//
//
//
//
//    //用户登陆逻辑
//    private static void userLoginStruct(DataStreamSource<ArrayList<Integer>> userLoginStream) {
//        SingleOutputStreamOperator<ArrayList<Row>> userLoginProcessedStream = userLoginStream.map(new MapFunction<ArrayList<Integer>, ArrayList<Row>>() {
//            @Override
//            public ArrayList<Row> map(ArrayList<Integer> value) throws Exception {
//                ArrayList<Row> userLoginList = new ArrayList();
//                for (Integer userId : value) {
//                    Row row = new Row(3);
//                    row.setField(0, userId);
//                    row.setField(1,1);
//                    row.setField(2, parseLong2StringNew(System.currentTimeMillis()));
//                    userLoginList.add(row);
//                }
//                return userLoginList;
//            }
//        });
//        userLoginProcessedStream.addSink(new UserLoginWriter());
//    }
//
//
//    //停止回访逻辑
//    private static void forbidenVisitStruct(DataStreamSource<ArrayList<Row>> userForbidenVisitStream) {
//        SingleOutputStreamOperator<ArrayList<Row>> forbidenVisitProcessedStream = userForbidenVisitStream.map(new MapFunction<ArrayList<Row>, ArrayList<Row>>() {
//            @Override
//            public ArrayList<Row> map(ArrayList<Row> value) throws Exception {
//                ArrayList<Row> forbidenVisitList = new ArrayList();
//                for (Row userReturnVisit : value) {
//                    Row row = new Row(4);
//                    row.setField(0, userReturnVisit.getField(0));//classId
//                    row.setField(1, userReturnVisit.getField(1));//userId
//                    row.setField(2, 1);//isForbidenVisit
//                    row.setField(3, parseLong2StringNew(System.currentTimeMillis()));
//                    forbidenVisitList.add(row);
//                }
//                return forbidenVisitList;
//            }
//        });
//        forbidenVisitProcessedStream.addSink(new UserForbidenVisitWriter());
//    }
//
//    //回访逻辑
//    private static void returnVisitStruct(DataStreamSource<ArrayList<Row>> userReturnVisitStream) {
//        SingleOutputStreamOperator<ArrayList<Row>> returnVisitProcessedStream = userReturnVisitStream.map(new MapFunction<ArrayList<Row>, ArrayList<Row>>() {
//            @Override
//            public ArrayList<Row> map(ArrayList<Row> value) throws Exception {
//                ArrayList<Row> returnVisitList = new ArrayList();
//                for (Row userReturnVisit : value) {
//                    Row row = new Row(5);
//                    row.setField(0, userReturnVisit.getField(0));//classId
//                    row.setField(1, userReturnVisit.getField(1));//userId
//                    row.setField(2, userReturnVisit.getField(2));//unitId
//                    row.setField(3, 1);//isVisit
//                    row.setField(4, parseLong2StringNew(System.currentTimeMillis()));
//                    returnVisitList.add(row);
//                }
//                return returnVisitList;
//            }
//        });
//        returnVisitProcessedStream.addSink(new UserReturnVisitWriter());
//    }
//
//
//    //点评作业逻辑
//    private static void correctStruct(DataStreamSource<String> correctStream
//            , BroadcastStream<ArrayList<ClassCourseAllPlot>> courseBroadcastStream
//            , MapStateDescriptor<String, ArrayList<ClassCourseAllPlot>> courseBroadCastDesc) {
//        SingleOutputStreamOperator<Row> correctProcessedStream = correctStream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String value) throws Exception {
//                String courseInfo = JSON.parseObject(value).getString("courseInfo");
//                if (JSON.parseObject(courseInfo) != null) {
//                    if (JSON.parseObject(courseInfo).containsKey("course_level")) {
//                        if (JSON.parseObject(courseInfo).getInteger("course_level") != null) {
//                            Integer course_level = JSON.parseObject(courseInfo).getInteger("course_level");
//                            String courseType = JSON.parseObject(value).getString("courseType");
//                            if (course_level.equals(1) && courseType.equals("Script")) {
//                                return true;
//                            }
//                        }
//                    }
//                }
//                return false;
//            }
//        }).map(new MapFunction<String, Row>() {
//            @Override
//            public Row map(String value) throws Exception {
//                Integer classId = JSON.parseObject(value).getInteger("classId");
//                Integer userId = JSON.parseObject(value).getInteger("userId");
//                Integer unitId = JSON.parseObject(value).getInteger("unitId");
//                Integer homeworkScore = JSON.parseObject(value).getInteger("score");
//                //logger.info("开始批改作业");
//
//                Row row = new Row(6);
//                row.setField(0, classId);
//                row.setField(1, userId);
//                row.setField(2, unitId);
//                row.setField(3, 1);
//                row.setField(4, homeworkScore);
//                row.setField(5, parseLong2StringNew(System.currentTimeMillis()));
//                return row;
//            }
//        }).connect(courseBroadcastStream)
//                .process(new BroadcastProcessFunction<Row, ArrayList<ClassCourseAllPlot>, Row>() {
//                    @Override
//                    public void processElement(Row value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
//                        ArrayList<ClassCourseAllPlot> classCourseAllPlots = ctx.getBroadcastState(courseBroadCastDesc).get("BroadCastStateKey");
//                        if (classCourseAllPlots!=null) {
//                            HashSet<Integer> classSet = new HashSet<>();
//                            for (ClassCourseAllPlot classCourseAllPlot : classCourseAllPlots) {
//                                classSet.add(classCourseAllPlot.getClassId());
//                            }
//                            //判断是否属于L1班级
//                            if (classSet.contains(value.getField(0))) {
//                                out.collect(value);
//                            }
//
//                            //取出缓存内容
//                            if (correctCache != null && correctCache.size() > 0) {
//                                synchronized (CounselorUserLearningStat.class) {
//                                    ListIterator<Row> rowIterator = correctCache.listIterator();
//                                    while (rowIterator.hasNext()) {
//                                        Row cache = rowIterator.next();
//                                        //判断是否属于L1班级
//                                        if (classSet.contains(cache.getField(0))) {
//                                            out.collect(cache);
//                                            rowIterator.remove();
//                                            logger.info("清空批改缓存ing... :" + correctCache.size());
//                                        }
//                                    }
//                                }
//                            }
//                        } else {
//                            correctCache.add(value);
//                            logger.info("存入批改缓存ing... :" + correctCache.size());
//                        }
//                    }
//
//                    @Override
//                    public void processBroadcastElement(ArrayList<ClassCourseAllPlot> value, Context ctx, Collector<Row> out) throws Exception {
//                        logger.info("批改 广播课程树:" + parseLong2StringNew(System.currentTimeMillis()));
//                        ctx.getBroadcastState(courseBroadCastDesc).put("BroadCastStateKey", value);
//                    }
//                });
//
//        correctProcessedStream.addSink(new CorrectInfoWriter());
//    }
//
//    //完课逻辑
//    private static void homeworkStruct(DataStream<String> homeworkStream
//            , BroadcastStream<ArrayList<ClassCourseAllPlot>> courseBroadcastStream
//            , MapStateDescriptor<String, ArrayList<ClassCourseAllPlot>> courseBroadCastDesc) {
//
//
//        SingleOutputStreamOperator<Row> homeworkProcessedStream = homeworkStream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String value) throws Exception {
//                if (JSON.parseObject(value).getJSONObject("homework").getInteger("type").equals(0)) {
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        }).map(new MapFunction<String, Row>() {
//            @Override
//            public Row map(String value) throws Exception {
//                String homework = JSON.parseObject(value).getString("homework");
//                Integer classId = JSON.parseObject(homework).getInteger("classId");
//                Integer userId = JSON.parseObject(homework).getInteger("userId");
//                Integer unitId = JSON.parseObject(homework).getInteger("unitId");
//                String status = JSON.parseObject(homework).getString("status");
//                String commitTime = parseLong2StringNew(Long.valueOf(JSON.parseObject(homework).getString("commitTime").substring(0, 13)));
//
//                Row row = new Row(7);
//                if (status.equals("PUBLISHED")) {
//                    //logger.info("作业完成1：classId:"+classId+" userId:"+userId+" unitId:"+unitId);
//                    row.setField(0, classId);
//                    row.setField(1, userId);
//                    row.setField(2, unitId);
//                    row.setField(3, 1);
//                    row.setField(4, commitTime);
//                    row.setField(5, 0);
//                    row.setField(6, parseLong2StringNew(System.currentTimeMillis()));
//                }
//                return row;
//            }
//        }).connect(courseBroadcastStream)
//                .process(new BroadcastProcessFunction<Row, ArrayList<ClassCourseAllPlot>, Row>() {
//                    @Override
//                    public void processElement(Row value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
//                        ArrayList<ClassCourseAllPlot> classCourseAllPlots = ctx.getBroadcastState(courseBroadCastDesc).get("BroadCastStateKey");
//                        if (classCourseAllPlots!=null) {
//                            HashSet<Integer> classSet = new HashSet<>();
//                            for (ClassCourseAllPlot classCourseAllPlot : classCourseAllPlots) {
//                                classSet.add(classCourseAllPlot.getClassId());
//                            }
//                            //判断是否属于L1班级
//                            if (classSet.contains(value.getField(0))) {
//                                //logger.info("作业完成：classId:" + value.getField(0) + " userId:" + value.getField(1) + " unitId:" + value.getField(2));
//                                out.collect(value);
//                            }
//
//
//                                //取出缓存内容
//                                if (homeworkCache != null && homeworkCache.size() > 0) {
//                                    synchronized (CounselorUserLearningStat.class) {
//                                    ListIterator<Row> rowIterator = homeworkCache.listIterator();
//                                    while (rowIterator.hasNext()) {
//                                        Row cache = rowIterator.next();
//                                        //判断是否属于L1班级
//                                        if (classSet.contains(cache.getField(0))) {
//                                            //logger.info("缓存-作业完成：classId:" + value.getField(0) + " userId:" + value.getField(1) + " unitId:" + value.getField(2));
//                                            out.collect(cache);
//                                            rowIterator.remove();
//                                            logger.info("清空作业缓存ing... :" + homeworkCache.size());
//                                        }
//                                    }
//                                }
//                            }
//                        } else {
//                            homeworkCache.add(value);
//                            logger.info("存入作业缓存ing... :" + homeworkCache.size());
//                        }
//                    }
//
//                    @Override
//                    public void processBroadcastElement(ArrayList<ClassCourseAllPlot> value, Context ctx, Collector<Row> out) throws Exception {
//                        logger.info("作业 广播课程树:" + parseLong2StringNew(System.currentTimeMillis()));
//                        ctx.getBroadcastState(courseBroadCastDesc).put("BroadCastStateKey", value);
//                    }
//                });
//        homeworkProcessedStream.addSink(new HwInfoWriter());
//    }
//
//
//    //进度逻辑
//    private static void progressStruct(DataStream<String> progressStream
//            , BroadcastStream<ArrayList<ClassCourseAllPlot>> courseBroadcastStream
//            , MapStateDescriptor<String, ArrayList<ClassCourseAllPlot>> courseBroadCastDesc
//            , BroadcastStream<HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastStream
//            , MapStateDescriptor<String, HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastDesc) {
//
//        SingleOutputStreamOperator<Row> progressProcessedStream = progressStream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String value) throws Exception {
//                if (JSON.parseObject(value)!=null) {
//                    if (!JSON.parseObject(value).getString("source").equals("ExtendedLesson")) {
//                        Integer courseLevel = JSON.parseObject(value).getJSONObject("course").getInteger("level");
//                        Integer id = JSON.parseObject(value).getInteger("id");
//                        if (courseLevel.equals(1) && !id.equals(0)) {
//                            return true;
//                        } else {
//                            return false;
//                        }
//                    } else {
//                        return false;
//                    }
//                }
//                return false;
//            }
//        }).map(new MapFunction<String, Tuple5<Integer, Integer, Integer, Integer, String>>() {
//            @Override
//            public Tuple5<Integer, Integer, Integer, Integer, String> map(String value) throws Exception {
//
//                //HashMap<Integer, Tuple4<Integer, Integer, Integer, String>> chapterUser = new HashMap<>();
//                Integer chapterId = JSON.parseObject(value).getInteger("chapterId");
//                Integer userId = JSON.parseObject(value).getInteger("userId");
//                Integer classId = JSON.parseObject(value).getInteger("classId");
//                Integer unitId = JSON.parseObject(value).getInteger("unitId");
//                String finishTime = parseLong2StringNew(Long.valueOf(JSON.parseObject(value).getString("finishTime")));
//                //logger.info("作业:classId="+classId+",userId="+userId+",unitId="+unitId+",chapterId="+chapterId+",finishTime="+finishTime);
//                //chapterUser.put(chapterId, new Tuple4(userId, classId, unitId,finishTime));
//                return new Tuple5(userId, classId, unitId, chapterId, finishTime);
//            }
//        }).connect(unlockCntBroadCastStream)
//                .process(new BroadcastProcessFunction<Tuple5<Integer, Integer, Integer, Integer, String>, HashMap<Integer, ArrayList<MainPlot>>, Row>() {
//                    @Override
//                    public void processElement(Tuple5<Integer, Integer, Integer, Integer, String> value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
//                        HashMap<Integer, ArrayList<MainPlot>> classMainPlots = ctx.getBroadcastState(unlockCntBroadCastDesc).get("BroadCastStateKey");
//
//                        if (classMainPlots != null) {
//                            if (classMainPlots.keySet().contains(value.f1)) {
//                                Row row = new Row(19);
//                                for (MainPlot mainPlot : classMainPlots.get(value.f1)) {
//                                    if (mainPlot.getUnitId().equals(value.f2)) {
//                                        row.setField(0, value.f3);//chapterId
//                                        row.setField(1, value.f4);//finishTime
//                                        row.setField(2, value.f0);//userId
//                                        row.setField(3, value.f1);//classId
//                                        row.setField(4, mainPlot.getClassName());
//                                        row.setField(5, mainPlot.getGrade());
//                                        row.setField(6, mainPlot.getClassType());
//                                        row.setField(7, mainPlot.getTermId());
//                                        row.setField(8, mainPlot.getTermName());
//                                        row.setField(9, mainPlot.getCounselorId());
//                                        row.setField(10, mainPlot.getCounselorName());
//                                        row.setField(11, mainPlot.getCourseLevel());
//                                        row.setField(12, value.f2);//unitId
//                                        row.setField(13, mainPlot.getUnitName());
//                                        row.setField(14, mainPlot.getUnitSequence());
//                                        row.setField(15, mainPlot.getUnitUnlockedTime());
//                                        row.setField(16, mainPlot.getHomeworkOpenCnt());
//                                        row.setField(17, mainPlot.getChallengeOpenCnt());
//                                        row.setField(18, mainPlot.getTotalOpenCnt());
//                                        out.collect(row);
//                                    }
//                                }
//                            }
//                            //取出缓存内容
//                            if (progressCache!=null && progressCache.size()>0) {
//                                synchronized (CounselorUserLearningStat.class) {
//                                    ListIterator<Row> tuple5ListIterator = progressCache.listIterator();
//                                    while (tuple5ListIterator.hasNext()) {
//                                        Row cache = tuple5ListIterator.next();
//                                        if (classMainPlots.keySet().contains(cache.getField(1))) {
//                                            Row row = new Row(19);
//                                            for (MainPlot mainPlot : classMainPlots.get(cache.getField(1))) {
//                                                if (mainPlot.getUnitId().equals(cache.getField(2))) {
//                                                    row.setField(0, cache.getField(3));//chapterId
//                                                    row.setField(1, cache.getField(4));//finishTime
//                                                    row.setField(2, cache.getField(0));//userId
//                                                    row.setField(3, cache.getField(1));//classId
//                                                    row.setField(4, mainPlot.getClassName());
//                                                    row.setField(5, mainPlot.getGrade());
//                                                    row.setField(6, mainPlot.getClassType());
//                                                    row.setField(7, mainPlot.getTermId());
//                                                    row.setField(8, mainPlot.getTermName());
//                                                    row.setField(9, mainPlot.getCounselorId());
//                                                    row.setField(10, mainPlot.getCounselorName());
//                                                    row.setField(11, mainPlot.getCourseLevel());
//                                                    row.setField(12, cache.getField(2));//unitId
//                                                    row.setField(13, mainPlot.getUnitName());
//                                                    row.setField(14, mainPlot.getUnitSequence());
//                                                    row.setField(15, mainPlot.getUnitUnlockedTime());
//                                                    row.setField(16, mainPlot.getHomeworkOpenCnt());
//                                                    row.setField(17, mainPlot.getChallengeOpenCnt());
//                                                    row.setField(18, mainPlot.getTotalOpenCnt());
//                                                    out.collect(row);
//                                                    tuple5ListIterator.remove();
//                                                    logger.info("清空进度缓存1... :" + progressCache.size());
//                                                }
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        }
//                        else {
//                            Row row = new Row(5);
//                            row.setField(0,value.f0);
//                            row.setField(1,value.f1);
//                            row.setField(2,value.f2);
//                            row.setField(3,value.f3);
//                            row.setField(4,value.f4);
//                            progressCache.add(row);
//                            logger.info("放入进度缓存1... :"+progressCache.size());
//                        }
//
//                    }
//                    @Override
//                    public void processBroadcastElement(HashMap<Integer, ArrayList<MainPlot>> value, Context ctx, Collector<Row> out) throws Exception {
//                        ctx.getBroadcastState(unlockCntBroadCastDesc).put("BroadCastStateKey", value);
//                        logger.info("发送 应完进度广播流:" + parseLong2StringNew(System.currentTimeMillis()));
//                    }
//                })
//
//                //计算分子 及完成时间
//                .connect(courseBroadcastStream)
//                .process(new BroadcastProcessFunction<Row, ArrayList<ClassCourseAllPlot>, Row>() {
//
//                    @Override
//                    public void processElement(Row value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
//                        ArrayList<ClassCourseAllPlot> courseCourseAllPlots = ctx.getBroadcastState(courseBroadCastDesc).get("BroadCastStateKey");
//
//                        if (courseCourseAllPlots!=null) {
//                            //<chapter_id,<tag,finish_tag,item_type>>
//                            HashMap<Integer, Tuple3<Integer, Integer, String>> chapterTag = new HashMap<>();
//
//                            //更新13个字段
//                            Row row = new Row(26);
//                            for (ClassCourseAllPlot classCourseAllPlot : courseCourseAllPlots) {
//                                //将全部chapterId对应的标签 全部放入Map
//                                chapterTag.put(classCourseAllPlot.getChapterId()
//                                        , new Tuple3(classCourseAllPlot.getTag(), classCourseAllPlot.getFinishTag(), classCourseAllPlot.getItemType()));
//                            }
//
//                            Integer chapterId = Integer.valueOf(value.getField(0).toString()) ;
//                            Tuple3<Integer, Integer, String> tag = chapterTag.get(chapterId);
//                            if (tag != null) {
//                                //logger.info("标签 tag:" + tag.f0 + ",finishTag:" + tag.f1 + ",itemType:" + tag.f2);
//                                if (tag.f2.equals("PROJECT") || tag.f2.equals("EXAM")) {
//                                    //logger.info("输出 挑战 userId="+value.getField(2)+",time="+value.getField(1)+",unitId="+value.getField(12));
//                                    row.setField(0, value.getField(3));
//                                    row.setField(1, value.getField(2));
//                                    row.setField(2, value.getField(12));
//                                    row.setField(3, value.getField(4));
//                                    row.setField(4, value.getField(5));
//                                    row.setField(5, value.getField(6));
//                                    row.setField(6, value.getField(7));
//                                    row.setField(7, value.getField(8));
//                                    row.setField(8, value.getField(9));
//                                    row.setField(9, value.getField(10));
//                                    row.setField(10, value.getField(11));
//                                    row.setField(11, value.getField(13));
//                                    row.setField(12, value.getField(14));
//                                    row.setField(13, value.getField(15));
//                                    row.setField(14, value.getField(16));
//                                    row.setField(15, value.getField(17));
//                                    row.setField(16, value.getField(18));
//                                    row.setField(17, 1);
//                                    row.setField(18, value.getField(1));
//                                    row.setField(19, 1);
//                                    row.setField(20, value.getField(1));
//                                    row.setField(21, value.getField(1));
//                                    row.setField(22, 0);
//                                    row.setField(23, "2099-01-01 00:00:00");
//                                    row.setField(24, "1970-01-01 00:00:00");
//                                    row.setField(25, parseLong2StringNew(System.currentTimeMillis()));
//                                    out.collect(row);
//                                }
//                                if (tag.f0.equals(0)) {
//                                    row.setField(0, value.getField(3));
//                                    row.setField(1, value.getField(2));
//                                    row.setField(2, value.getField(12));
//                                    row.setField(3, value.getField(4));
//                                    row.setField(4, value.getField(5));
//                                    row.setField(5, value.getField(6));
//                                    row.setField(6, value.getField(7));
//                                    row.setField(7, value.getField(8));
//                                    row.setField(8, value.getField(9));
//                                    row.setField(9, value.getField(10));
//                                    row.setField(10, value.getField(11));
//                                    row.setField(11, value.getField(13));
//                                    row.setField(12, value.getField(14));
//                                    row.setField(13, value.getField(15));
//                                    row.setField(14, value.getField(16));
//                                    row.setField(15, value.getField(17));
//                                    row.setField(16, value.getField(18));
//                                    row.setField(17, 1);
//                                    row.setField(18, value.getField(1));
//                                    row.setField(19, 0);
//                                    row.setField(20, "2099-01-01 00:00:00");
//                                    row.setField(21, "1970-01-01 00:00:00");
//                                    row.setField(22, 1);
//                                    row.setField(23, value.getField(1));
//                                    row.setField(24, value.getField(1));
//                                    row.setField(25, parseLong2StringNew(System.currentTimeMillis()));
//                                    out.collect(row);
//                                }
//                            }
//
//                            //再次消费缓存消息
//                            if (progressCache2.size()>0 && progressCache2!=null) {
//                                synchronized (CounselorUserLearningStat.class) {
//                                    ListIterator<Row> rowListIterator = progressCache2.listIterator();
//                                    while (rowListIterator.hasNext()) {
//                                        Row cache = rowListIterator.next();
//                                        //Integer chapterId = Integer.valueOf(cache.getField(0).toString()) ;
//                                        Tuple3<Integer, Integer, String> tag2 = chapterTag.get(Integer.valueOf(cache.getField(0).toString()));
//                                        if (tag2 != null) {
//                                            if (tag2.f2.equals("PROJECT") || tag2.f2.equals("EXAM")) {
//                                                logger.info("缓存——输出 挑战: userId=" + cache.getField(2) + ",time=" + cache.getField(1) + ",unitId=" + cache.getField(12) + ",chapterId=" + cache.getField(0));
//                                                row.setField(0, cache.getField(3));
//                                                row.setField(1, cache.getField(2));
//                                                row.setField(2, cache.getField(12));
//                                                row.setField(3, cache.getField(4));
//                                                row.setField(4, cache.getField(5));
//                                                row.setField(5, cache.getField(6));
//                                                row.setField(6, cache.getField(7));
//                                                row.setField(7, cache.getField(8));
//                                                row.setField(8, cache.getField(9));
//                                                row.setField(9, cache.getField(10));
//                                                row.setField(10, cache.getField(11));
//                                                row.setField(11, cache.getField(13));
//                                                row.setField(12, cache.getField(14));
//                                                row.setField(13, cache.getField(15));
//                                                row.setField(14, cache.getField(16));
//                                                row.setField(15, cache.getField(17));
//                                                row.setField(16, cache.getField(18));
//                                                row.setField(17, 1);
//                                                row.setField(18, cache.getField(1));
//                                                row.setField(19, 1);
//                                                row.setField(20, cache.getField(1));
//                                                row.setField(21, cache.getField(1));
//                                                row.setField(22, 0);
//                                                row.setField(23, "2099-01-01 00:00:00");
//                                                row.setField(24, "1970-01-01 00:00:00");
//                                                row.setField(25, parseLong2StringNew(System.currentTimeMillis()));
//                                                out.collect(row);
//                                            }
//                                            if (tag2.f0.equals(0)) {
//                                                logger.info("缓存——输出 通关: userId=" + cache.getField(2) + ",time=" + cache.getField(1) + ",unitId=" + cache.getField(12) + ",chapterId=" + cache.getField(0));
//                                                row.setField(0, cache.getField(3));
//                                                row.setField(1, cache.getField(2));
//                                                row.setField(2, cache.getField(12));
//                                                row.setField(3, cache.getField(4));
//                                                row.setField(4, cache.getField(5));
//                                                row.setField(5, cache.getField(6));
//                                                row.setField(6, cache.getField(7));
//                                                row.setField(7, cache.getField(8));
//                                                row.setField(8, cache.getField(9));
//                                                row.setField(9, cache.getField(10));
//                                                row.setField(10, cache.getField(11));
//                                                row.setField(11, cache.getField(13));
//                                                row.setField(12, cache.getField(14));
//                                                row.setField(13, cache.getField(15));
//                                                row.setField(14, cache.getField(16));
//                                                row.setField(15, cache.getField(17));
//                                                row.setField(16, cache.getField(18));
//                                                row.setField(17, 1);
//                                                row.setField(18, cache.getField(1));
//                                                row.setField(19, 0);
//                                                row.setField(20, "2099-01-01 00:00:00");
//                                                row.setField(21, "1970-01-01 00:00:00");
//                                                row.setField(22, 1);
//                                                row.setField(23, cache.getField(1));
//                                                row.setField(24, cache.getField(1));
//                                                row.setField(25, parseLong2StringNew(System.currentTimeMillis()));
//                                                out.collect(row);
//                                            }
//                                        }
//                                        rowListIterator.remove();
//                                        logger.info("清空进度缓存2... :" + progressCache2.size());
//                                    }
//                                }
//                            }
//                        }
//                        else {
//                            progressCache2.add(value);
//                            logger.info("放入进度缓存2... :"+progressCache2.size());
//                        }
//                    }
//                    @Override
//                    public void processBroadcastElement(ArrayList<ClassCourseAllPlot> value, Context ctx, Collector<Row> out) throws Exception {
//                        logger.info("发送 课程树广播流:" + parseLong2StringNew(System.currentTimeMillis()));
//                        ctx.getBroadcastState(courseBroadCastDesc).put("BroadCastStateKey", value);
//                    }
//                });
//        progressProcessedStream.addSink(new FinishCntWriter());
//    }
//
//}