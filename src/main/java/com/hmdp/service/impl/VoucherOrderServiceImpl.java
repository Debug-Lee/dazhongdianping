package com.hmdp.service.impl;


import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWoker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWoker redisIdWoker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;


    //静态代码块，加载lua脚本文件
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua")); //设置Lua脚本位置
        SECKILL_SCRIPT.setResultType(Long.class);    //设置返回的类型
    }


    //异步处理线程池
    //从线程池获得单线程用来异步处理秒杀订单业务
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //在类初始化了之后执行，因为这个类初始化了之后，随时可能抢单
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOderHandler());
    }


    private class VoucherOderHandler implements  Runnable{
        private String streamName = "stream.orders";
        @Override
        public void run() {
            while(true){
                try {
                    //1、获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(streamName, ReadOffset.lastConsumed())
                    );
                    //2、判断订单是否为空
                    if(list == null || list.isEmpty()){
                        //为空，则继续下一次循环
                        continue;
                    }
                    //3、创建订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    //去除ID，获得值
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    //4、确认消息 XACK stream.orders g1 ID
                    stringRedisTemplate.opsForStream().acknowledge(streamName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    //处理异常
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    //1、获取pendding-list 中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1  STREAMS s1 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(streamName, ReadOffset.from("0"))
                    );
                    //2、判断订单是否为空
                    if(list == null || list.isEmpty()){
                        //为空，则就结束循环
                        break;
                    }
                    //3、创建订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    //去除ID，获得值
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    //4、确认消息 XACK stream.orders g1 ID
                    stringRedisTemplate.opsForStream().acknowledge(streamName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pendding订单异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }

        public void handleVoucherOrder(VoucherOrder voucherOrder){
            Long voucherId = voucherOrder.getVoucherId();
            Long userId = voucherOrder.getUserId();
            //利用redis实现分布式锁
            //1、创建simpleRedisLock对象
            //减少锁的力度，锁的是 某个用户抢某个优惠券的锁
            RLock lock = redissonClient.getLock("lock:order" + userId + ":" + voucherId);
            //2、获取锁
            boolean isLock = lock.tryLock();
            //3、如果获取失败
            if(!isLock){
                //同一个用户用同一个线程，如果获取失败，说明已经下过单
                log.error("不允许重复下单！");
                return;
            }
            //4、获取成功
            try {
                //注意：由于是spring事务是放在threadLocal中，此时的是子线程，事务会失效
                proxy.createVoucherOrder(voucherOrder);
            }finally {
                lock.unlock();
            }

        }
    }

//    //阻塞队列，存储订单信息
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
//    // 用于线程池处理的任务
//    // 当初始化完毕后，就会去从对列中去拿信息
//    private class VoucherOderHandler implements  Runnable{
//        @Override
//        public void run() {
//            while(true){
//                try {
//                    //1、获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //2、创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                    log.error("处理订单异常",e);
//                }
//            }
//        }
//
//        public void handleVoucherOrder(VoucherOrder voucherOrder){
//            Long voucherId = voucherOrder.getVoucherId();
//            Long userId = voucherOrder.getUserId();
//            //利用redis实现分布式锁
//            //1、创建simpleRedisLock对象
//            RLock lock = redissonClient.getLock("lock:order" + userId + ":" + voucherId);
//            //2、获取锁
//            boolean isLock = lock.tryLock();
//            //3、如果获取失败
//            if(!isLock){
//                //同一个用户用同一个线程，如果获取失败，说明已经下过单
//                log.error("不允许重复下单！");
//                return;
//            }
//            //4、获取成功
//            try {
//                //注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
//                proxy.createVoucherOrder(voucherOrder);
//            }finally {
//                lock.unlock();
//            }
//
//        }
//    }


    //将代理对象变为类成员变量，这样在异步下单的子线程里面，就可以用代理对象执行方法，保证事务生效
    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //3.2 获取订单ID
        Long orderId = redisIdWoker.nextId("order");

        //1、运行lua脚本，判断是否有资格
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),orderId.toString());
        int r = result.intValue();
        //2、判断结果是否为0
        if(r != 0){
            return Result.fail(r == 1? "库存不足":"不允许重复下单");
        }
        //4.创建代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1、查询优惠券信息
//        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
//        //2、是否到开始时间
//        //2、1是否在开始之前
//        if(seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("未到开始时间！");
//        }
//
//        //2、2是否在开始之后
//        if(seckillVoucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("抢购已经结束！");
//        }
//        //3、是否有库存
//        if(seckillVoucher.getStock() < 1){
//            return Result.fail("已经被抢完了！");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        //intern() 这个方法是从常量池中拿到数据，如果我们直接使用userId.toString()
//        // 他拿到的对象实际上是不同的对象，new出来的对象，我们使用锁必须保证锁必须是同把，所以我们需要使用intern()一方法
//
////        synchronized (userId.toString().intern()){  //给当前事务方法添加锁
////            //获取代理对象，才能调用带有事务的方法，不然事务会失效
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.createVoucherOrder(seckillVoucher.getVoucherId());
////        }
//        //利用redis实现分布式锁
//        //1、创建simpleRedisLock对象
////        simpleRedisLock simpleRedisLock = new simpleRedisLock("order" + userId + ":" + voucherId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order" + userId + ":" + voucherId);
//        //2、获取锁
//        boolean isLock = lock.tryLock();
//        //3、如果获取失败
//        if(!isLock){
//            //同一个用户用同一个线程，如果获取失败，说明已经下过单
//            return  Result.fail("不允许重复下单！");
//        }
//        //4、获取成功
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(seckillVoucher.getVoucherId());
//        }finally {
//            lock.unlock();
//        }
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        //4、一人一单
        //查找当前数据库中 当前用户是否对某个优惠券已经下单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if(count > 0){  //如果数据库中已存在，则说明已经抢购过，返回
            log.error("当前用户已经抢购过！");
        }

        //4、如果有库存
        //删除库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")    //set stock = stock -1
                .eq("voucher_id", voucherId)    //where voucher_id = voucherId and stock = stock
                .gt("stock",0)  //where voucher_id = voucherId and stock > stock  乐观锁优化
                .update();
        if(!success){
            log.error("已经被抢完了！");
        }
        //6、保存并返回结果
        save(voucherOrder);
    }
}
