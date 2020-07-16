package example;

import com.esotericsoftware.kryo.io.Output;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Iterator;


@Slf4j
public class Main1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(5000);
        //并行度设置为 1
        env.setParallelism(1);
//        env.setParallelism(4);

        SingleOutputStreamOperator<Tuple2<String, Long> > data = env
                .socketTextStream("bigdata1", 9000)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Tuple2<String, Long>(split[0], Long.parseLong(split[1]));
                    }
                }).assignTimestampsAndWatermarks(new WordPeriodicWatermark());
        OutputTag<Tuple2<String,Long>> last = new OutputTag<Tuple2<String,Long>>("last"){};

        data.keyBy(0)
                .timeWindow(Time.seconds(10),Time.seconds(5))
                .sideOutputLateData(last)
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Object, Tuple, TimeWindow>() {
                    private int i = 0;
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Object> out) throws Exception {
                        Long min = null;
                        for (Iterator<Tuple2<String, Long>> it = elements.iterator();it.hasNext();){
                            i++;
                            Tuple2<String, Long> value = it.next();
                            log.info("元素【{}】,【{}】",value.getField(0),value.getField(1));
                            log.info("当前waterMarker【{}】,当前窗口【{}】",context.currentWatermark(),context.window().toString());
                        }

                        out.collect(i);
                        i = 0;
                    }
                }).print();
        DataStream<Tuple2<String,Long>> out =  data.getSideOutput(last);
        out.print();
        env.execute("watermark demo");
    }
}
