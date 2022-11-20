package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWoker {
    /**
     * 开始的时间戳
     */
    public static final long BEGIN_TIMESTAMP =  1640995200L;

    /**
     * 序列号的位数
     */
    public static final int COUNT_BITS =  32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWoker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public Long nextId(String keyPrefix){
        //1、生成当前时间的时间戳
        LocalDateTime nowTime = LocalDateTime.now();
        long stamp = nowTime.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = stamp - BEGIN_TIMESTAMP;

        //2、生成序列号
        //2.1 获取当前日期
        String date = nowTime.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //2.2 自增长
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        //3、拼接并返回
        return timeStamp << COUNT_BITS | count;
    }

}
