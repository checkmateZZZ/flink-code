package com.hetao101.SupervisionAnalyse.flink.tutor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hetao101.SupervisionAnalyse.common.Constants;
import com.hetao101.SupervisionAnalyse.conf.ConfigManager;
import com.hetao101.SupervisionAnalyse.entity.MainPlot;
import com.hetao101.SupervisionAnalyse.reader.ClassInfoReader;
import com.hetao101.SupervisionAnalyse.reader.tutor.TutorChapterTypeInfoReader;
import com.hetao101.SupervisionAnalyse.reader.tutor.TutorUnlockCntReaderHive;
import com.hetao101.SupervisionAnalyse.writer.tutor.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hetao101.SupervisionAnalyse.util.TimeUtil.parseLong2StringNew;

public class TutorUserLearningStat implements Serializable {
    private static Logger logger = Logger.getLogger(TutorUserLearningStat.class);
    public static ArrayList<Row> progressCache = new ArrayList();
    public static ArrayList<Row> progressCache2 = new ArrayList();
    public static ArrayList<Row> homeworkCache = new ArrayList();
    public static ArrayList<Row> correctCache = new ArrayList();
    //public static ArrayList<Row> ucCache = new ArrayList();

    public static void main(String[] args) throws Exception {

        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://10.100.7.251:8020/flink/flink-checkpoints",true));
        //失败重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
              5, //重启次数
              Time.of(10, TimeUnit.SECONDS) //间隔
        ));
        env.setParallelism(4);

        //kafka配置信息
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", ConfigManager.getProperty(Constants.BOOTSTRAP_SEVERS_1));
        kafkaProp.setProperty("group.id", Constants.GROUP_ID);

        Properties ucKafkaProp = new Properties();
        ucKafkaProp.setProperty("bootstrap.servers", ConfigManager.getProperty(Constants.BOOTSTRAP_SEVERS_2));
        ucKafkaProp.setProperty("group.id", Constants.GROUP_ID);


        //应完课数广播描述器
        MapStateDescriptor<String, HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastDesc =
                new MapStateDescriptor("UnlockCntBroadCastStream"
                        , BasicTypeInfo.STRING_TYPE_INFO
                        , TypeInformation.of(new TypeHint<HashMap<Integer, ArrayList<MainPlot>>>() {
                }));
        BroadcastStream<HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastStream =
                env.addSource(new TutorUnlockCntReaderHive()).broadcast(unlockCntBroadCastDesc);

        //课程树广播描述器
        MapStateDescriptor<String, HashMap<Integer, HashMap<Integer, Tuple2>>> courseBroadCastDesc =
                new MapStateDescriptor("CourseBroadCastStream"
                        , BasicTypeInfo.STRING_TYPE_INFO
                        , TypeInformation.of(new TypeHint<HashMap<Integer, HashMap<Integer, Tuple2>>>() {
                }));
        BroadcastStream<HashMap<Integer, HashMap<Integer, Tuple2>>> courseBroadCastStream =
                env.addSource(new TutorChapterTypeInfoReader()).broadcast(courseBroadCastDesc);

        //班级维表广播描述器
        MapStateDescriptor<String, HashMap<Integer, Row>> classBroadCastDesc =
                new MapStateDescriptor("ClassBroadCastStream"
                        , BasicTypeInfo.STRING_TYPE_INFO
                        , TypeInformation.of(new TypeHint<HashMap<Integer, Row>>() {
                }));
        BroadcastStream<HashMap<Integer, Row>> classBroadCastStream =
                env.addSource(new ClassInfoReader()).broadcast(classBroadCastDesc);

        //获取用户调班流
//        DataStreamSource<String> ucStream =
//                env.addSource(new FlinkKafkaConsumer010("hetao-user-class", new SimpleStringSchema(), ucKafkaProp).setStartFromTimestamp(Long.valueOf("1583769600000")));

        //获取人课关系
        //DataStreamSource<ArrayList<Row>> userClassCourseCnt = env.addSource(new TutorUserClassCourseCntReader());

        //获取课程进度流
        DataStreamSource<String> progressStream =
                env.addSource(new FlinkKafkaConsumer010("hetao-learning-progress", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1584889200000")));

        //获取作业流
        DataStreamSource<String> homeworkStream =
                env.addSource(new FlinkKafkaConsumer010("hetao-submit-homework", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1584889200000")));

        //获取到课流
        DataStreamSource<String> attendanceStream =
                env.addSource(new FlinkKafkaConsumer010("hetao-attendance", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1584889200000")));

        //批改作业流
        DataStreamSource<String> correctStream =
                env.addSource(new FlinkKafkaConsumer010("hetao-correct-homework", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1584889200000")));
        //correctStream.print();

        //提交测试题流
        DataStreamSource<String> examAnswerStream =
                env.addSource(new FlinkKafkaConsumer010("hetao-submit-examAnswer", new SimpleStringSchema(), kafkaProp).setStartFromTimestamp(Long.valueOf("1584889200000")));
        //examAnswerStream.print();



        //人课关系
        //userClassCourseCntStruct(userClassCourseCnt);

        //进度、到课逻辑
        progressStruct(progressStream, courseBroadCastStream,courseBroadCastDesc,unlockCntBroadCastStream,unlockCntBroadCastDesc);

        //完课逻辑
        homeworkStruct(homeworkStream,classBroadCastStream,classBroadCastDesc);

        //点评作业逻辑
        correctStruct(correctStream,classBroadCastStream,classBroadCastDesc);

        //随堂测试题
        examAnswerStruct(examAnswerStream);

        //开始执行
        env.execute();

    }

    private static void examAnswerStruct(DataStreamSource<String> examAnswerStream) {
        SingleOutputStreamOperator<ArrayList<Row>> examAnswerProcessedStream = examAnswerStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                JSONObject examAnswer = JSONObject.parseObject(value).getJSONObject("examAnswer");
                if (examAnswer.getString("examType").equals("QUIZ")){
                    return true;
                }
                return false;
            }
        }).map(new MapFunction<String, ArrayList<Row>>() {
            @Override
            public ArrayList<Row> map(String value) throws Exception {
                JSONObject examAnswer = JSONObject.parseObject(value).getJSONObject("examAnswer");
                Long commitTime = JSONObject.parseObject(value).getLong("commitTime");
                Integer userId = examAnswer.getInteger("userId");
                Integer classId = examAnswer.getInteger("classId");
                Integer courseId = examAnswer.getInteger("courseId");
                Integer courseLevel = examAnswer.getInteger("courseLevel");
                Integer unitId = examAnswer.getInteger("unitId");
                Integer unitSequence = examAnswer.getInteger("unitSequence");
                Integer chapterId = examAnswer.getInteger("chapterId");
                Integer submissionId = examAnswer.getInteger("submission_id");
                Integer examId = examAnswer.getInteger("examId");
                Integer externalId = examAnswer.getInteger("externalId");
                Integer examScore = examAnswer.getInteger("score");
                Integer costTime = examAnswer.getInteger("costTime");

                ArrayList<Row> answerRows = new ArrayList<>();
                JSONArray submissionDetails = examAnswer.getJSONArray("submissionDetails");
                //System.out.println("userId:"+userId+",classId:"+classId+",chapterId:"+chapterId+",总题数"+submissionDetails.size()+",提交时间:"+parseLong2StringNew(commitTime));
                //System.out.println(value.toString());
                for (int i = 0; i < submissionDetails.size(); i++) {
                    Row row = new Row(21);
                    Integer questionId = submissionDetails.getJSONObject(i).getInteger("questionId");
                    String userAnswer = submissionDetails.getJSONObject(i).getString("userAnswer");
                    String correctAnswer = submissionDetails.getJSONObject(i).getString("answer");
                    Integer examResult = submissionDetails.getJSONObject(i).getInteger("result");
                    //System.out.println("questionId:"+questionId+",题号:"+(i+1));
                    //System.out.println(i + 1);
                    row.setField(0, userId);
                    row.setField(1, classId);
                    row.setField(2, courseId);
                    row.setField(3, courseLevel);
                    row.setField(4, unitId);
                    row.setField(5, unitSequence);
                    row.setField(6, chapterId);
                    row.setField(7, submissionId);
                    row.setField(8, examId);
                    row.setField(9, externalId);
                    row.setField(10, 1);
                    row.setField(11, examScore);
                    row.setField(12, costTime);
                    row.setField(13, 1);
                    row.setField(14, questionId);
                    row.setField(15, i + 1);
                    row.setField(16, examResult);
                    row.setField(17, parseLong2StringNew(commitTime));
                    row.setField(18, userAnswer);
                    row.setField(19, correctAnswer);
                    row.setField(20, parseLong2StringNew(System.currentTimeMillis()));

                    answerRows.add(row);
                }
                //System.out.println("插入条数："+answerRows.size());
                return answerRows;
            }
        });
        examAnswerProcessedStream.addSink(new ExamAnswerWriter());
        examAnswerProcessedStream.addSink(new ExamAnswerWriter2());
    }


    //点评作业逻辑
    private static void correctStruct(DataStreamSource<String> correctStream
            , BroadcastStream<HashMap<Integer, Row>> classBroadCastStream
            , MapStateDescriptor<String, HashMap<Integer, Row>> classBroadCastDesc) {
        SingleOutputStreamOperator<Row> correctProcessedStream = correctStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                String courseInfo = JSON.parseObject(value).getString("courseInfo");
                if (JSON.parseObject(courseInfo) != null) {
                    if (JSON.parseObject(courseInfo).containsKey("course_level")) {
                        if (JSON.parseObject(courseInfo).getInteger("course_level") != null) {
                            Integer course_level = JSON.parseObject(courseInfo).getInteger("course_level");
                            String courseType = JSON.parseObject(value).getString("courseType");
                            //if (course_level.equals(1) && courseType.equals("Script")) {
                                return true;
                            //}
                        }
                    }
                }
                return false;
            }
        }).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                Integer classId = JSON.parseObject(value).getInteger("classId");
                Integer userId = JSON.parseObject(value).getInteger("userId");
                Integer unitId = JSON.parseObject(value).getInteger("unitId");
                Integer homeworkScore = JSON.parseObject(value).getInteger("score");
                //logger.info("开始批改作业");

                Row row = new Row(6);
                row.setField(0, classId);
                row.setField(1, userId);
                row.setField(2, unitId);
                row.setField(3, 1);
                row.setField(4, homeworkScore);
                row.setField(5, parseLong2StringNew(System.currentTimeMillis()));
                return row;
            }
        }).connect(classBroadCastStream)
                .process(new BroadcastProcessFunction<Row, HashMap<Integer, Row>, Row>() {
                    @Override
                    public void processElement(Row value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
                        HashMap<Integer, Row> classInfos = ctx.getBroadcastState(classBroadCastDesc).get("BroadCastStateKey");
                        if (classInfos!=null) {
                            if (classInfos.containsKey(value.getField(0))) {
                                Integer courseGroup = Integer.valueOf(classInfos.get(value.getField(0)).getField(3).toString());
                                //判断是否属于年课、季课班级
                                if (courseGroup.equals(2) || courseGroup.equals(3)) {
                                    out.collect(value);
                                }
                            }

                            //取出缓存内容
                            if (correctCache != null && correctCache.size() > 0) {
                                synchronized (TutorUserLearningStat.class) {
                                    ListIterator<Row> rowIterator = correctCache.listIterator();
                                    while (rowIterator.hasNext()) {
                                        Row cache = rowIterator.next();
                                        if (classInfos.containsKey(cache.getField(0))) {
                                            Integer courseGroupp = Integer.valueOf(classInfos.get(cache.getField(0)).getField(3).toString());
                                            //判断是否属于年课、季课班级
                                            if (courseGroupp.equals(2) || courseGroupp.equals(3)) {
                                                out.collect(cache);
                                                rowIterator.remove();
                                                logger.info("清空批改缓存ing... :" + correctCache.size());
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            correctCache.add(value);
                            logger.info("存入批改缓存ing... :" + correctCache.size());
                        }
                    }

                    @Override
                    public void processBroadcastElement(HashMap<Integer, Row> value, Context ctx, Collector<Row> out) throws Exception {
                        logger.info("批改 广播课程树:" + parseLong2StringNew(System.currentTimeMillis()));
                        ctx.getBroadcastState(classBroadCastDesc).put("BroadCastStateKey", value);
                    }
                });

        correctProcessedStream.addSink(new TutorCorrectInfoWriter());
    }

    //完课逻辑
    private static void homeworkStruct(DataStream<String> homeworkStream
            , BroadcastStream<HashMap<Integer, Row>> classBroadCastStream
            , MapStateDescriptor<String, HashMap<Integer, Row>> classBroadCastDesc) {


        SingleOutputStreamOperator<Row> homeworkProcessedStream = homeworkStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (JSON.parseObject(value).getJSONObject("homework").getInteger("type").equals(0)) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String homework = JSON.parseObject(value).getString("homework");
                Integer classId = JSON.parseObject(homework).getInteger("classId");
                Integer userId = JSON.parseObject(homework).getInteger("userId");
                Integer unitId = JSON.parseObject(homework).getInteger("unitId");
                String status = JSON.parseObject(homework).getString("status");
                String commitTime = parseLong2StringNew(Long.valueOf(JSON.parseObject(homework).getString("commitTime").substring(0, 13)));

                Row row = new Row(7);
                if (status.equals("PUBLISHED")) {
                    //logger.info("作业完成1：classId:"+classId+" userId:"+userId+" unitId:"+unitId);
                    row.setField(0, classId);
                    row.setField(1, userId);
                    row.setField(2, unitId);
                    row.setField(3, 1);
                    row.setField(4, commitTime);
                    row.setField(5, 0);
                    row.setField(6, parseLong2StringNew(System.currentTimeMillis()));
                }
                return row;
            }
        }).connect(classBroadCastStream)
                .process(new BroadcastProcessFunction<Row, HashMap<Integer, Row>, Row>() {
                    @Override
                    public void processElement(Row value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
                        HashMap<Integer, Row> classInfos = ctx.getBroadcastState(classBroadCastDesc).get("BroadCastStateKey");
                        if (classInfos!=null) {
                            //System.out.println(value.toString());
                            if (classInfos.containsKey(value.getField(0))) {
                                Integer courseGroup = Integer.valueOf(classInfos.get(value.getField(0)).getField(3).toString());
                                //判断是否属于年课、季课班级
                                if (courseGroup.equals(2) || courseGroup.equals(3)) {
                                    out.collect(value);
                                }
                            }

                            //取出缓存内容
                            if (homeworkCache != null && homeworkCache.size() > 0) {
                                synchronized (TutorUserLearningStat.class) {
                                    ListIterator<Row> rowIterator = homeworkCache.listIterator();
                                    while (rowIterator.hasNext()) {
                                        Row cache = rowIterator.next();
                                        if (classInfos.containsKey(cache.getField(0))) {
                                            Integer courseGroupp = Integer.valueOf(classInfos.get(cache.getField(0)).getField(3).toString());
                                            //判断是否属于年课、季课班级
                                            if (courseGroupp.equals(2) || courseGroupp.equals(3)) {
                                                out.collect(cache);
                                                rowIterator.remove();
                                                logger.info("清空作业缓存ing... :" + homeworkCache.size());
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            homeworkCache.add(value);
                            logger.info("存入作业缓存ing... :" + homeworkCache.size());
                        }
                    }

                    @Override
                    public void processBroadcastElement(HashMap<Integer, Row> value, Context ctx, Collector<Row> out) throws Exception {
                        logger.info("作业 广播课程树:" + parseLong2StringNew(System.currentTimeMillis()));
                        ctx.getBroadcastState(classBroadCastDesc).put("BroadCastStateKey", value);
                    }
                });
        homeworkProcessedStream.addSink(new TutorHwInfoWriter());
    }


    //学习进度逻辑
    private static void progressStruct(DataStream<String> progressStream
            , BroadcastStream<HashMap<Integer, HashMap<Integer, Tuple2>>> courseBroadcastStream
            , MapStateDescriptor<String, HashMap<Integer, HashMap<Integer, Tuple2>>> courseBroadCastDesc
            , BroadcastStream<HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastStream
            , MapStateDescriptor<String, HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastDesc) {

        SingleOutputStreamOperator<Row> progressProcessedStream = progressStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                if (JSON.parseObject(value)!=null) {
                    if (!JSON.parseObject(value).getString("source").equals("ExtendedLesson")) {
                        //Integer courseLevel = JSON.parseObject(value).getJSONObject("course").getInteger("level");
                        Integer id = JSON.parseObject(value).getInteger("id");
                        if (!id.equals(0)) {
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                return false;
            }
        }).map(new MapFunction<String, Tuple5<Integer, Integer, Integer, Integer, String>>() {
            @Override
            public Tuple5<Integer, Integer, Integer, Integer, String> map(String value) {

                //HashMap<Integer, Tuple4<Integer, Integer, Integer, String>> chapterUser = new HashMap<>();
                Integer chapterId = JSON.parseObject(value).getInteger("chapterId");
                Integer userId = JSON.parseObject(value).getInteger("userId");
                Integer classId = JSON.parseObject(value).getInteger("classId");
                Integer unitId = JSON.parseObject(value).getInteger("unitId");
                String finishTime = parseLong2StringNew(Long.valueOf(JSON.parseObject(value).getString("finishTime")));
                //logger.info("作业:classId="+classId+",userId="+userId+",unitId="+unitId+",chapterId="+chapterId+",finishTime="+finishTime);
                //chapterUser.put(chapterId, new Tuple4(userId, classId, unitId,finishTime));
                return new Tuple5(userId, classId, unitId, chapterId, finishTime);
            }
        }).connect(unlockCntBroadCastStream)
                .process(new BroadcastProcessFunction<Tuple5<Integer, Integer, Integer, Integer, String>, HashMap<Integer, ArrayList<MainPlot>>, Row>() {
                    @Override
                    public void processElement(Tuple5<Integer, Integer, Integer, Integer, String> value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
                        HashMap<Integer, ArrayList<MainPlot>> classMainPlots = ctx.getBroadcastState(unlockCntBroadCastDesc).get("BroadCastStateKey");

                        if (classMainPlots != null) {
                            if (classMainPlots.containsKey(value.f1)) {
                                Row row = new Row(19);
                                for (MainPlot mainPlot : classMainPlots.get(value.f1)) {
                                    if (mainPlot.getUnitId().equals(value.f2)) {
                                        row.setField(0, value.f3);//chapterId
                                        row.setField(1, value.f4);//finishTime
                                        row.setField(2, value.f0);//userId
                                        row.setField(3, value.f1);//classId
                                        row.setField(4, mainPlot.getClassName());
                                        row.setField(5, mainPlot.getGrade());
                                        row.setField(6, mainPlot.getClassType());
                                        row.setField(7, mainPlot.getTermId());
                                        row.setField(8, mainPlot.getTermName());
                                        row.setField(9, mainPlot.getCounselorId());
                                        row.setField(10, mainPlot.getCounselorName());
                                        row.setField(11, mainPlot.getCourseLevel());
                                        row.setField(12, value.f2);//unitId
                                        row.setField(13, mainPlot.getUnitSequence());
                                        row.setField(14, mainPlot.getUnitUnlockedTime());
                                        row.setField(15, mainPlot.getHomeworkOpenCnt());
                                        row.setField(16, mainPlot.getChallengeOpenCnt());
                                        row.setField(17, mainPlot.getTotalOpenCnt());
                                        row.setField(18, mainPlot.getCoursePackageId());
                                        out.collect(row);
                                    }
                                }
                            }
                            //取出缓存内容
                            if (progressCache!=null && progressCache.size()>0) {
                                synchronized (TutorUserLearningStat.class) {
                                    ListIterator<Row> tuple5ListIterator = progressCache.listIterator();
                                    while (tuple5ListIterator.hasNext()) {
                                        Row cache = tuple5ListIterator.next();
                                        //if(cache!=null){
                                        if (classMainPlots.containsKey(cache.getField(1))) {
                                            Row row = new Row(19);
                                            for (MainPlot mainPlot : classMainPlots.get(cache.getField(1))) {
                                                if (mainPlot.getUnitId().equals(cache.getField(2))) {
                                                    row.setField(0, cache.getField(3));//chapterId
                                                    row.setField(1, cache.getField(4));//finishTime
                                                    row.setField(2, cache.getField(0));//userId
                                                    row.setField(3, cache.getField(1));//classId
                                                    row.setField(4, mainPlot.getClassName());
                                                    row.setField(5, mainPlot.getGrade());
                                                    row.setField(6, mainPlot.getClassType());
                                                    row.setField(7, mainPlot.getTermId());
                                                    row.setField(8, mainPlot.getTermName());
                                                    row.setField(9, mainPlot.getCounselorId());
                                                    row.setField(10, mainPlot.getCounselorName());
                                                    row.setField(11, mainPlot.getCourseLevel());
                                                    row.setField(12, cache.getField(2));//unitId
                                                    row.setField(13, mainPlot.getUnitSequence());
                                                    row.setField(14, mainPlot.getUnitUnlockedTime());
                                                    row.setField(15, mainPlot.getHomeworkOpenCnt());
                                                    row.setField(16, mainPlot.getChallengeOpenCnt());
                                                    row.setField(17, mainPlot.getTotalOpenCnt());
                                                    row.setField(18, mainPlot.getCoursePackageId());
                                                    out.collect(row);
                                                    tuple5ListIterator.remove();
                                                    logger.info("清空进度缓存1... :" + progressCache.size());
                                                }
                                            }
                                        //}
                                        }
                                    }
                                }
                            }
                        }
                        else {
                            Row row = new Row(5);
                            row.setField(0,value.f0);
                            row.setField(1,value.f1);
                            row.setField(2,value.f2);
                            row.setField(3,value.f3);
                            row.setField(4,value.f4);
                            progressCache.add(row);
                            logger.info("放入进度缓存1... :"+progressCache.size());
                        }

                    }
                    @Override
                    public void processBroadcastElement(HashMap<Integer, ArrayList<MainPlot>> value, Context ctx, Collector<Row> out) throws Exception {
                        ctx.getBroadcastState(unlockCntBroadCastDesc).put("BroadCastStateKey", value);
                        logger.info("发送 应完进度广播流:" + parseLong2StringNew(System.currentTimeMillis()));
                    }
                })

                //计算分子 及完成时间
                .connect(courseBroadcastStream)
                .process(new BroadcastProcessFunction<Row, HashMap<Integer, HashMap<Integer, Tuple2>>, Row>() {

                    @Override
                    public void processElement(Row value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
                        //<unitId,<<chapterId,<itemType,tag>>>>
                        HashMap<Integer, HashMap<Integer, Tuple2>> chapterTypeInfos = ctx.getBroadcastState(courseBroadCastDesc).get("BroadCastStateKey");

                        if (chapterTypeInfos!=null) {

                            Integer unitId = Integer.valueOf(value.getField(12).toString());
                            Integer chapterId = Integer.valueOf(value.getField(0).toString());

                            Tuple2 tuple2 = chapterTypeInfos.get(unitId).get(chapterId);
                            if (tuple2 != null) {
                                Row row = new Row(26);
                                //logger.info("标签 tag:" + tag.f0 + ",finishTag:" + tag.f1 + ",itemType:" + tag.f2);
                                if (tuple2.f0.equals("PROJECT") || tuple2.f0.equals("EXAM")) {
                                    //logger.info("输出 挑战 userId="+value.getField(2)+",time="+value.getField(1)+",unitId="+value.getField(12));
                                    row.setField(0, value.getField(3));
                                    row.setField(1, value.getField(2));
                                    row.setField(2, value.getField(12));
                                    row.setField(3, value.getField(4));
                                    row.setField(4, value.getField(5));
                                    row.setField(5, value.getField(6));
                                    row.setField(6, value.getField(7));
                                    row.setField(7, value.getField(8));
                                    row.setField(8, value.getField(9));
                                    row.setField(9, value.getField(10));
                                    row.setField(10, value.getField(11));
                                    row.setField(11, value.getField(13));
                                    row.setField(12, value.getField(14));
                                    row.setField(13, value.getField(15));
                                    row.setField(14, value.getField(16));
                                    row.setField(15, value.getField(17));
                                    row.setField(16, 1);
                                    row.setField(17, value.getField(1));
                                    row.setField(18, 1);
                                    row.setField(19, value.getField(1));
                                    row.setField(20, value.getField(1));
                                    row.setField(21, 0);
                                    row.setField(22, "2099-01-01 00:00:00");
                                    row.setField(23, "1970-01-01 00:00:00");
                                    row.setField(24, parseLong2StringNew(System.currentTimeMillis()));
                                    row.setField(25, value.getField(18));
                                    out.collect(row);
                                }
                                if (tuple2.f1.equals(0)) {
                                    row.setField(0, value.getField(3));
                                    row.setField(1, value.getField(2));
                                    row.setField(2, value.getField(12));
                                    row.setField(3, value.getField(4));
                                    row.setField(4, value.getField(5));
                                    row.setField(5, value.getField(6));
                                    row.setField(6, value.getField(7));
                                    row.setField(7, value.getField(8));
                                    row.setField(8, value.getField(9));
                                    row.setField(9, value.getField(10));
                                    row.setField(10, value.getField(11));
                                    row.setField(11, value.getField(13));
                                    row.setField(12, value.getField(14));
                                    row.setField(13, value.getField(15));
                                    row.setField(14, value.getField(16));
                                    row.setField(15, value.getField(17));
                                    row.setField(16, 1);
                                    row.setField(17, value.getField(1));
                                    row.setField(18, 0);
                                    row.setField(19, "2099-01-01 00:00:00");
                                    row.setField(20, "1970-01-01 00:00:00");
                                    row.setField(21, 1);
                                    row.setField(22, value.getField(1));
                                    row.setField(23, value.getField(1));
                                    row.setField(24, parseLong2StringNew(System.currentTimeMillis()));
                                    row.setField(25, value.getField(18));
                                    out.collect(row);
                                }
                            }

                            //再次消费缓存消息
                            if (progressCache2.size()>0 && progressCache2!=null) {
                                synchronized (TutorUserLearningStat.class) {
                                    ListIterator<Row> rowListIterator = progressCache2.listIterator();
                                    while (rowListIterator.hasNext()) {
                                        Row cache = rowListIterator.next();
                                        Integer unitId2 = Integer.valueOf(cache.getField(12).toString());
                                        Integer chapterId2 = Integer.valueOf(cache.getField(0).toString());

                                        Tuple2 tupleCache = chapterTypeInfos.get(unitId2).get(chapterId2);
                                        if (tupleCache != null) {
                                            Row row = new Row(26);
                                            if (tupleCache.f0.equals("PROJECT") || tupleCache.f0.equals("EXAM")) {
                                                logger.info("缓存——输出 挑战: userId=" + cache.getField(2) + ",time=" + cache.getField(1) + ",unitId=" + cache.getField(12) + ",chapterId=" + cache.getField(0));
                                                row.setField(0, cache.getField(3));
                                                row.setField(1, cache.getField(2));
                                                row.setField(2, cache.getField(12));
                                                row.setField(3, cache.getField(4));
                                                row.setField(4, cache.getField(5));
                                                row.setField(5, cache.getField(6));
                                                row.setField(6, cache.getField(7));
                                                row.setField(7, cache.getField(8));
                                                row.setField(8, cache.getField(9));
                                                row.setField(9, cache.getField(10));
                                                row.setField(10, cache.getField(11));
                                                row.setField(11, cache.getField(13));
                                                row.setField(12, cache.getField(14));
                                                row.setField(13, cache.getField(15));
                                                row.setField(14, cache.getField(16));
                                                row.setField(15, cache.getField(17));
                                                row.setField(16, 1);
                                                row.setField(17, cache.getField(1));
                                                row.setField(18, 1);
                                                row.setField(19, cache.getField(1));
                                                row.setField(20, cache.getField(1));
                                                row.setField(21, 0);
                                                row.setField(22, "2099-01-01 00:00:00");
                                                row.setField(23, "1970-01-01 00:00:00");
                                                row.setField(24, parseLong2StringNew(System.currentTimeMillis()));
                                                row.setField(25, cache.getField(18));
                                                out.collect(row);
                                            }
                                            if (tupleCache.f1.equals(0)) {
                                                logger.info("缓存——输出 通关: userId=" + cache.getField(2) + ",time=" + cache.getField(1) + ",unitId=" + cache.getField(12) + ",chapterId=" + cache.getField(0));
                                                row.setField(0, cache.getField(3));
                                                row.setField(1, cache.getField(2));
                                                row.setField(2, cache.getField(12));
                                                row.setField(3, cache.getField(4));
                                                row.setField(4, cache.getField(5));
                                                row.setField(5, cache.getField(6));
                                                row.setField(6, cache.getField(7));
                                                row.setField(7, cache.getField(8));
                                                row.setField(8, cache.getField(9));
                                                row.setField(9, cache.getField(10));
                                                row.setField(10, cache.getField(11));
                                                row.setField(11, cache.getField(13));
                                                row.setField(12, cache.getField(14));
                                                row.setField(13, cache.getField(15));
                                                row.setField(14, cache.getField(16));
                                                row.setField(15, cache.getField(17));
                                                row.setField(16, 1);
                                                row.setField(17, cache.getField(1));
                                                row.setField(18, 0);
                                                row.setField(19, "2099-01-01 00:00:00");
                                                row.setField(20, "1970-01-01 00:00:00");
                                                row.setField(21, 1);
                                                row.setField(22, cache.getField(1));
                                                row.setField(23, cache.getField(1));
                                                row.setField(24, parseLong2StringNew(System.currentTimeMillis()));
                                                row.setField(25, cache.getField(18));
                                                out.collect(row);
                                            }
                                        }
                                        rowListIterator.remove();
                                        logger.info("清空进度缓存2... :" + progressCache2.size());
                                    }
                                }
                            }
                        }
                        else {
                            progressCache2.add(value);
                            logger.info("放入进度缓存2... :"+progressCache2.size());
                        }
                    }
                    @Override
                    public void processBroadcastElement(HashMap<Integer, HashMap<Integer, Tuple2>> value, Context ctx, Collector<Row> out) throws Exception {
                        logger.info("发送 课程树广播流:" + parseLong2StringNew(System.currentTimeMillis()));
                        ctx.getBroadcastState(courseBroadCastDesc).put("BroadCastStateKey", value);
                    }
                });
        progressProcessedStream.addSink(new TutorFinishCntWriter());
    }


//    //获取人课关系
//    public static void userClassCourseCntStruct(DataStreamSource<ArrayList<Row>> userClassCourseCnt) {
//
//        SingleOutputStreamOperator<ArrayList<Row>> userClassCourseCntStream = userClassCourseCnt.map(new MapFunction<ArrayList<Row>, ArrayList<Row>>() {
//            @Override
//            public ArrayList<Row> map(ArrayList<Row> value) throws Exception {
//                System.out.println(value.toString());
//                return value;
//            }
//        });
//        //插入mysql
//        userClassCourseCntStream.addSink(new TutorUserClassCourseCntWriter());
//    }

}
