package com.hmdp.utils;


import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //判断用户是否存在
        if(UserHolder.getUser() == null){
            //如果当前没有用户信息，说明没有登录，拦截
            response.setStatus(401);
            return false;
        }
        //有用户,放行
        return true;
    }
}
