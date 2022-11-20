package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPLIST_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private IShopTypeService typeService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result selectTypeList() {
        String key = CACHE_SHOPLIST_KEY;
        //1、查是否有缓存
        String shopListInfo = stringRedisTemplate.opsForValue().get(key);
        //2、如果有，则返回
        if(StrUtil.isNotBlank(shopListInfo)){
            List<ShopType> shopTypes = JSONUtil.toList(shopListInfo, ShopType.class);
            return Result.ok(shopTypes);
        }
        //3、如果没有，则查数据库
        List<ShopType> typeList = typeService
                .query().orderByAsc("sort").list();
        //4、如果没查到，则返回错误
        if(typeList == null){
            return Result.fail("未找到商品类型！");
        }
        //5、如果查到，则加入redis，并返回  //设置时间，定期清理
        shopListInfo = JSONUtil.toJsonStr(typeList);
        stringRedisTemplate.opsForValue().set(key,shopListInfo);
        return Result.ok(typeList);
    }
}
