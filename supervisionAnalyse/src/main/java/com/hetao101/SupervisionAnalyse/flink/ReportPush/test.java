package com.hetao101.SupervisionAnalyse.flink.ReportPush;

import com.hetao101.SupervisionAnalyse.dao.reader.UnlockCntReaderPresto;
import com.hetao101.SupervisionAnalyse.entity.MainPlot;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashMap;

public class test {
    public static void main(String[] args) throws Exception {
//        Tuple2<Integer, Integer> integerIntegerTuple2 = new Tuple2<>(2, 3);
//        logger.info(integerIntegerTuple2);

        //logger.info(new Date().getTime());//1578650362338
        //logger.info(Calendar.getInstance().getWeekYear());
//
//        logger.info(new Date(Long.valueOf("1478650362338")));
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeInMillis(Long.valueOf("1478650362338"));
//        logger.info(calendar.getTime());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MapStateDescriptor<String,HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastDesc =
                new MapStateDescriptor("UnlockCntBroadCastStream"
                        , BasicTypeInfo.STRING_TYPE_INFO
                        , TypeInformation.of(new TypeHint<HashMap<Integer, ArrayList<MainPlot>>>() {
                }));
        BroadcastStream<HashMap<Integer, ArrayList<MainPlot>>> unlockCntBroadCastStream =
                env.addSource(new UnlockCntReaderPresto()).broadcast(unlockCntBroadCastDesc);


        env.execute();
    }
}
