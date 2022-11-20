package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.BeanUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    @Override
    public Result follow(Long id, Boolean isFollow) {
        //1、获取当前登录用户ID
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        //2、判断是否关注还是取消关注
        if(isFollow){
            //3、关注 新增数据 insert into follow
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(id);
            boolean isSuccess = save(follow);
            //Redis set中添加关注的人
            if(isSuccess){
                stringRedisTemplate.opsForSet().add(key,id.toString());
            }
        }else{
            //4、取消关注 删除数据 delete from follow where user_id = userId and follow_user_id = followUserId
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", id)
            );
            if(isSuccess){
                stringRedisTemplate.opsForSet().remove(key,id.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long id) {
        //1、获取当前登录用户ID
        Long userId = UserHolder.getUser().getId();

        //2、查询表中是否有对应数据 select count(*) from follow where user_id = userId and follow_user_id = followUserId
        Integer count = query().eq("user_id", userId).eq("follow_user_id", id).count();

        //3、判断是否有数据，有数据则为true
        return Result.ok(count > 0);
    }

    @Override
    public Result commonFollows(Long id) {
        //1、获取当前用户id
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        String key2 = "follows:" + id;
        //2、获取目标ID，查询共同交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if(intersect == null || intersect.isEmpty()){
            //无交集
            return  Result.ok(Collections.emptyList());
        }
        //3、解析ID集合
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //4、查询用户
        List<UserDTO> users = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //5、返回结果
        return Result.ok(users);
    }
}
