package com.hmdp.utils;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_TOKEN_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_TOKEN_TTL;

public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1、获取Headers
        String token = request.getHeader("authorization");
        //2、判断token是否为空
        if(StrUtil.isBlank(token)){
            return true;
        }
        // 从redis中获取user
        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(LOGIN_TOKEN_KEY + token);
        //3、判断用户是否存在
        if(entries.isEmpty()){
            return true;
        }
        //4 将map转换成user对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(entries, new UserDTO(), false);
        //5、存在，将用户存入ThreadLocal
        UserHolder.saveUser(userDTO);
        //6、刷新token有效期   保证只要访问页面，token就不会过期
        stringRedisTemplate.expire(LOGIN_TOKEN_KEY + token,LOGIN_TOKEN_TTL, TimeUnit.MINUTES);
        //放行
        return true;
    }


    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
