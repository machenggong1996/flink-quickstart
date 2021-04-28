package org.example.window;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author machenggong
 * @since 2021/4/28
 */
public class MyCountAggregate implements AggregateFunction<MyCountAggregate.ProductViewData, Long, Long> {


    @Override
    public Long createAccumulator() {
        /*访问量初始化为0*/
        return 0L;
    }

    @Override
    public Long add(ProductViewData value, Long accumulator) {
        /*访问量直接+1 即可*/
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    /*合并两个统计量*/
    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }

    public static class ProductViewData {

    }

}
