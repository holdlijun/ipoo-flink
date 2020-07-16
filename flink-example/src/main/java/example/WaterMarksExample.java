package example;

import common.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;


import javax.annotation.Nullable;

import java.util.Iterator;

import static common.DateUtil.YYYY_MM_DD_HH_MM_SS;
@Slf4j
public class WaterMarksExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //设置为eventtime事件类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置水印生成时间间隔100ms
//        env.getConfig().setAutoWatermarkInterval(100);
        env.setParallelism(1);
        // AssignerWithPunctuatedWatermarks：数据流中每一个递增的 EventTime 都会产生一个 Watermark。
        // AssignerWithPeriodicWatermarks：周期性的（一定时间间隔或者达到一定的记录条数）产生一个 Watermark。
        SingleOutputStreamOperator<Tuple2<String, Long>> dataStream = env
                .socketTextStream("bigdata1", 9000)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple2<String, Long>(split[0], Long.parseLong(split[1]));
                    }
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                    private Long currentTimeStamp = Long.MIN_VALUE;
                    //设置允许乱序时间
                    private Long maxOutOfOrderness = 5000L;

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimeStamp - maxOutOfOrderness);
                    }
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> s, long l) {

                        long timeStamp = s.f1;
                        this.currentTimeStamp = timeStamp;
//                        this.currentTimeStamp = Math.max(timeStamp,currentTimeStamp);
                        log.info("event timestamp = {}, {}, CurrentWatermark = {}, {}", timeStamp,
                                DateUtil.format(timeStamp, YYYY_MM_DD_HH_MM_SS),
                                getCurrentWatermark().getTimestamp(),
                                DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));

                        return timeStamp;

                    }
                });
        dataStream
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Integer, Tuple, TimeWindow>() {
            private int i = 0;
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Integer> out) throws Exception {
                Long min = null;
                for (Iterator<Tuple2<String, Long>> it = elements.iterator(); it.hasNext();){
                    i++;
                    Tuple2<String, Long> value = it.next();
                    log.info("元素【{}】,【{}】",value.getField(0),value.getField(1));
                    log.info("当前waterMarker【{}】,当前窗口【{}】",context.currentWatermark(),context.window().toString());
                }

                out.collect(i);
                i = 0;
            }
        }).print();
        env.execute("WaterMark Test Demo");

    }

}
