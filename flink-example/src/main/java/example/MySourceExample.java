package example;

import common.Item;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import source.MyStremingSource;

/**
 * @author admin
 */
public class MySourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Item> itmes = env.addSource(new MyStremingSource()).setParallelism(1);

//        DataStream<Object> item = itmes
//                .map(new MapFunction<MyStremingSource.Item, Object>() {
//                    @Override
//                    public Object map(MyStremingSource.Item value) throws Exception {
//                        return value.getName();
//                    }
//                });
        //TODO 使用SingleOutputStreamOperator 做map算子的输出 (lambda 表达式)
//        SingleOutputStreamOperator<Object> singleOutputStreamOperator = itmes
//                .map(singleMap -> singleMap.getName());

        //TODO 使用FilterFunction 保留id为偶数的
        SingleOutputStreamOperator<Item> singleOutputStreamOperator2 =itmes.filter(new FilterFunction<Item>() {
            @Override
            public boolean filter(Item value) throws Exception {
                return value.getId() % 2 == 0;
                // lambda  item -> item.getId() % 2 == 0
            }
        });

        //TODO 使用FlatMapFunction 过滤掉id>20的数据
        SingleOutputStreamOperator<Object> singleOutputStreamOperator1 = singleOutputStreamOperator2
                .flatMap(new FlatMapFunction<Item, Object>() {
                    @Override
                    public void flatMap(Item item1, Collector<Object> out) throws Exception {
                        if (item1.getId() < 20) {
                            out.collect(item1);
                        }
                    }
                });
        singleOutputStreamOperator1.print().setParallelism(1);
        env.execute("user defined streaming source");
    }
}
