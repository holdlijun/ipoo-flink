package example;

import com.sun.org.apache.xml.internal.resolver.Catalog;
import common.Item;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import source.MyStremingSource;

import java.util.ArrayList;
import java.util.List;


public class TableStremingDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        SingleOutputStreamOperator<Item> source = bsEnv.addSource(new MyStremingSource())
                .map(new MapFunction<Item, Item>() {
                    @Override
                    public Item map(Item value) throws Exception {
                        return value;
                    }
                });
        //TODO 分割流
        final OutputTag<Item> even = new OutputTag<Item>("even") {
        };
        final OutputTag<Item> old = new OutputTag<Item>("old") {
        };

        SingleOutputStreamOperator<Item> sideOutputData = source.process(new ProcessFunction<Item, Item>() {
            @Override
            public void processElement(Item value, Context ctx, Collector<Item> out) throws Exception {
                if (value.getId() % 2 == 0) {
                    ctx.output(even,value);
                }else{
                    ctx.output(old,value);
                }
            }
        });


        DataStream<Item> evenS = sideOutputData.getSideOutput(even);
        DataStream<Item> oldS = sideOutputData.getSideOutput(old);

        bsTableEnv.registerDataStream("evenTable",evenS , "name,id");
        bsTableEnv.registerDataStream("oddTable", oldS, "name,id");


        Table queryTable = bsTableEnv.sqlQuery("select a.id,a.name,b.id,b.name from evenTable as a join oddTable as b on a.name = b.name");
        queryTable.printSchema();;
        DataStream<Tuple2<Boolean, Tuple4<Integer, String, Integer, String>>> dataStream = bsTableEnv.toRetractStream(queryTable, TypeInformation.of(new TypeHint<Tuple4<Integer,String,Integer,String>>(){}));
        dataStream.print();

        bsEnv.execute("demo");
    }
}
