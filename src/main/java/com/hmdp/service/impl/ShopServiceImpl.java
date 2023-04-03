package com.hmdp.service.impl;

import cn.hutool.Hutool;
import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.injector.methods.SelectById;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.DEFAULT_PAGE_SIZE;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryShopById(Long id) {
        //通过缓存击穿查询数据
        //Shop shop = queryWithPassThrough(id);

        Shop shop = queryWithMutex(id);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }


    //基于缓存穿透，加入互斥锁进行缓存击穿
    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1、查询缓存
        String shopInfo = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopInfo)){
            //2、存在
            return JSONUtil.toBean(shopInfo, Shop.class);
        }
        //如果查到该ID缓存空字符串，说明数据库中没有，返回fail
        if(shopInfo != null){
            return null;
        }

        //3、不存在
        Shop shop = null;
        String lockKey = LOCK_SHOP_TTL + id;
        try {
            //3、1 获取锁
            boolean lock = getLock(lockKey);
            //3、2 判断是否获取到锁
            if(!lock){
                //3、3 没有获取到
                //线程等待，然后重新查询缓存
                Thread.sleep(50);
                //递归调用查询方法
                return queryWithMutex(id);
            }
            //4、获取到锁，查询数据库,判断是否有数据
            shop = getById(id);
            //模拟重建延时
            Thread.sleep(200);
            if(shop == null){
                //解决缓存击穿问题
                //如果查询数据库没得到，则向redis对应key中存一个空值
                stringRedisTemplate.opsForValue().set(key,"",CACHE_SHOP_NULL_TTL,TimeUnit.MINUTES);
                //5、如果没有，返回错误
                return null;
            }
            //6、如果有，将数据添加到redis中，并返回
            shopInfo = JSONUtil.toJsonStr(shop);
            stringRedisTemplate.opsForValue().set(key,shopInfo,CACHE_SHOP_TTL, TimeUnit.MINUTES);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            //7、释放锁
            unlock(lockKey);
        }
        return shop;
    }

    //获取互斥锁
    private boolean getLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10L, TimeUnit.MILLISECONDS);
        //自动拆箱有可能出现空指针，调用Hutool工具判断是否正确
        //自动拆箱
        return BooleanUtil.isTrue(flag);
    }

    //释放互斥锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    //封装，缓存击穿的方法
    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1、查询缓存
        String shopInfo = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopInfo)){
            //2、存在
            return JSONUtil.toBean(shopInfo, Shop.class);
        }
        //如果查到该ID缓存空字符串，说明数据库中没有，返回fail
        if(shopInfo != null){
            return null;
        }

        //3、不存在
        //4、查询数据库,判断是否有数据
        Shop shop = getById(id);
        if(shop == null){
            //解决缓存击穿问题
            //如果查询数据库没得到，则向redis对应key中存一个空值
            stringRedisTemplate.opsForValue().set(key,"",CACHE_SHOP_NULL_TTL,TimeUnit.MINUTES);
            //5、如果没有，返回错误
            return null;
        }
        //6、如果有，将数据添加到redis中，并返回
        shopInfo = JSONUtil.toJsonStr(shop);
        stringRedisTemplate.opsForValue().set(key,shopInfo,CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺ID不能为空");
        }
        //1、修改数据库
        updateById(shop);
        //2、删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1、判断是否需要查询定位信息
        if(x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        //2、计算分页参数
        int from = (current - 1) * DEFAULT_PAGE_SIZE;
        int end  =  current * DEFAULT_PAGE_SIZE;

        String key = SHOP_GEO_KEY + typeId;
        //3、从redis里面查出对应的商店ID和距离当前位置的距离
        //  GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        //4、解析出id 和 距离
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size() <= from){
            //没有下一页，返回空数据
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>();
        //4、1 截取id skip 跳过前面的n条数据
        list.stream().skip(from).forEach(result ->{
            //获取id
            String idStr = result.getContent().getName();
            //将id存入 ids
            ids.add(Long.valueOf(idStr));
            //获取distance
            Distance distance = result.getDistance();
            //添加id 和 距离的映射
            distanceMap.put(idStr,distance);
        });
        //5、查询商铺信息   返回结果有序
        //Order BY FIELD 按照id顺序去排序
        String idStr = StrUtil.join(",",ids);
        List<Shop> shopList = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        //6、将对应距离封装进数据里面
        for (Shop shop : shopList) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());  //getValue 将Distance对象变为distance
        }
        //7、返回结果
        return Result.ok(shopList);
    }
}
