package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.service.IShopService;
import com.hmdp.service.IShopTypeService;
import com.hmdp.utils.RedisIdWoker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedisIdWoker redisIdWoker;

    @Autowired
    private IShopService shopService;

    //开启500个线程执行任务
    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);//控制分线程先执行

        Runnable task = () -> { //创建task任务，每个线程生成100个ID
            for(int i = 0;i < 100;i++){
                long id = redisIdWoker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();  //countDown - 1 ，控制分线程执行完
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();//将主线程阻塞，等到所有的分线程执行完才会唤醒
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));//输出整个执行全过程花费的时间
    }

    @Test
    void StringTest(){
        stringRedisTemplate.opsForValue().set("name","李浩");
        String name = stringRedisTemplate.opsForValue().get("name");
        System.out.println("name = " + name);
    }

    @Test
    void Time(){
        LocalDateTime now = LocalDateTime.now();
        System.out.println("now = " + now);
    }

    @Test
    void importShop(){
        //1、查询所有店铺
        List<Shop> list = shopService.list();
        //2、把店铺分组，按照typeId分组，typeId一致的放到一个集合
        Map<Long,List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //3、批量插入
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //3.1 获取店铺类型
            Long shopType = entry.getKey();
            String key = SHOP_GEO_KEY + shopType;
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>();
            //3.2 获取该类型的店铺
            List<Shop> value = entry.getValue();
            //3.3 存入redis  geoadd key x y member
            for (Shop shop : value) {
                // 一个一个存入，连接次数多，效率低
                // stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(),shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

    @Test
    void UVCount(){
        String[] users = new String[1000];
        int j = 0;
        for (int i = 0; i < 1000000; i++) {
            j = i % 1000;
            users[j] = "user_" + i;
            if(j == 999){
                stringRedisTemplate.opsForHyperLogLog().add("hy1",users);
            }
        }
        Long size = stringRedisTemplate.opsForHyperLogLog().size("hy1");
        System.out.println(size);
    }


}
