package source;

import common.Product;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

public class ProductStremingSource implements SourceFunction<Product> {
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Product> ctx) throws Exception {
        while (isRunning){
            // 每一秒钟产生一条数据
            Product product = generateProduct();
            ctx.collect(product);
            Thread.sleep(1000);
        }
    }

    private Product generateProduct(){
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList();
        list.add("spring");
        list.add("summer");
        list.add("autumn");
        list.add("winter");
        Product product = new Product();
        product.setSeasonType(list.get(new Random().nextInt(4)));
        product.setId(i);
        return product;
    }
    @Override
    public void cancel() {

    }
}
