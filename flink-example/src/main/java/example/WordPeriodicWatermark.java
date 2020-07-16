package example;

import common.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import static common.DateUtil.YYYY_MM_DD_HH_MM_SS;


/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class WordPeriodicWatermark implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(Tuple2<String, Long> word, long previousElementTimestamp) {
        this.currentTimestamp = word.f1;
        log.info("event timestamp = {}, {}, CurrentWatermark = {}, {}", word.f1,
                DateUtil.format(word.f1, YYYY_MM_DD_HH_MM_SS),
                getCurrentWatermark().getTimestamp(),
                DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));
        return word.f1;

//        if (word.getTimestamp() > currentTimestamp) {
//            this.currentTimestamp = word.getTimestamp();
//        }
//        return currentTimestamp;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 3000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }
}
