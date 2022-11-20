package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import com.hmdp.service.ILock;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class simpleRedisLock implements ILock {

    private String name;    //加锁的业务名称
    private StringRedisTemplate stringRedisTemplate;    //stringRedisTemplate去操作
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-"; //生成UUID并去除-
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    //静态代码块，加载lua脚本文件
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua")); //设置Lua脚本位置
        UNLOCK_SCRIPT.setResultType(Long.class);    //设置返回的类型
    }

    public simpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        String threadId = ID_PREFIX + Thread.currentThread().getId(); //获取当前线程ID 加上 JVM生成的UUID，作为Key，区别集群JVM
        Boolean success = stringRedisTemplate
                .opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.MINUTES);
        return Boolean.TRUE.equals(success);
    }

    /**
     * 调用lua脚本，实现原子性的释放锁
     */
    @Override
    public void unlock() {
        //调用lua脚本
        stringRedisTemplate.execute(UNLOCK_SCRIPT   //设置脚本
                , Collections.singletonList(KEY_PREFIX + name)  //设置key，即锁的Key
                ,ID_PREFIX + Thread.currentThread().getId());   //设置与结果对比的线程表示
    }


    /**
     * 没有保证原子性的释放锁
     */
//    @Override
//    public void unlock() {
//        //释放锁的时候，判断是否是自己对应线程的锁，如果不是则不释放
//        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
//        String threadId = ID_PREFIX + Thread.currentThread().getId();
//        //判断线程标识是否一致
//        if(threadId.equals(id)){
//            //一致，则释放锁，否则不释放
//            stringRedisTemplate.delete(KEY_PREFIX + name);
//        }
//    }
}
