package example;

import common.Item;
import common.Product;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import source.ProductStremingSource;

import java.util.ArrayList;
import java.util.List;

public class OutputStremingDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Product> source = env.addSource(new ProductStremingSource());

        // 使用Side Output分流
        final OutputTag<Product> spring = new OutputTag<Product>("spring"){};
        final OutputTag<Product> summer = new OutputTag<Product>("summer"){};
        final OutputTag<Product> autumn = new OutputTag<Product>("autumn"){};
        final OutputTag<Product> winter = new OutputTag<Product>("winter"){};
        SingleOutputStreamOperator<Product> sideOutputData = source.process(new ProcessFunction<Product, Product>() {
            @Override
            public void processElement(Product product, Context ctx, Collector<Product> out) throws Exception {
                String seasonType = product.getSeasonType();
                switch (seasonType){
                    case "spring":
                        ctx.output(spring,product);
                        break;
                    case "summer":
                        ctx.output(summer,product);
                        break;
                    case "autumn":
                        ctx.output(autumn,product);
                        break;
                    case "winter":
                        ctx.output(winter,product);
                        break;
                    default:
                        out.collect(product);
                }
            }
        });

        DataStream<Product> springStream = sideOutputData.getSideOutput(spring);
        DataStream<Product> summerStream = sideOutputData.getSideOutput(summer);
        DataStream<Product> autumnStream = sideOutputData.getSideOutput(autumn);
        DataStream<Product> winterStream = sideOutputData.getSideOutput(winter);

        // 输出标签为:winter 的数据流
        winterStream.print();

        env.execute("output");
    }
}
