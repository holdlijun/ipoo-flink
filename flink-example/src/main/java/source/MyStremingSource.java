package source;

import common.Item;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

public class MyStremingSource implements SourceFunction<Item> {

    private boolean isRunning = true;


    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning){
            Item item = generateItem();
            ctx.collect(item);
            Thread.sleep(1000);
        }
    }

    /**
     * 随机产生一条记录
     *
     * @return
     */
    private Item generateItem(){
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }

    @Override
    public void cancel() {

    }


}
