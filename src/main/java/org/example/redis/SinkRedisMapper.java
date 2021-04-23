package org.example.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author machenggong
 * @since 2021/4/23
 */
public class SinkRedisMapper implements RedisMapper<Tuple2<String, Integer>> {


    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "flink");
    }

    @Override
    public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2.f1.toString();
    }
}
