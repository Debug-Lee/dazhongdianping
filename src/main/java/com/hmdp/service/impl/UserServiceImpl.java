package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.text.Format;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1、检验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            //2、如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        //3、符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        //4、存储code到redis中,并设置key的有效时间
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5、发送验证码
        log.debug("发送短信验证码成功，验证码：{}",code);
        //返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        //1、验证手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            //2、如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        //2、校验验证码
        String caechCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        //3、如果验证码不对，错误信息
        if(caechCode == null || !caechCode.equals(code)){
            return Result.fail("验证码错误");
        }
        //4、查询该手机号是否对应用户 select * from tb_user where phone = ?;
        User user = query().eq("phone", phone).one();

        //5、如果不存在，新建一个用户
        if(user == null){
            user = createUser(phone);
        }
        //6、存在，将用户信息存入redis
        //6.1 随机生成token,作为登陆令牌
        String token = UUID.randomUUID().toString(true);
        //6.2 将user对象转为hashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //因为使用的是stringRedisTemplate,kev value 都是String类型
        // copy对象为map的时候，将user中 long类型的值，转换为String类型的值
        Map<String, Object> map = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true)
                        .setFieldValueEditor((filedName,filedValue) -> filedValue.toString()));
        //6.3 存储
        String tokenKey = LOGIN_TOKEN_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,map);

        //6.4 设置有效时间
        stringRedisTemplate.expire(tokenKey,LOGIN_TOKEN_TTL, TimeUnit.MINUTES);

        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //1、获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2、获取当前时间
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        //3、设置key
        String key = USER_SIGN_KEY + userId + ":" + keySuffix;
        //4、获取当前是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5、签到
        Boolean isSign = stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);

        if(isSign){//如果签到成功，返回值为0，签到失败返回值为1
            return Result.fail("签到失败");
        }
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1、获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2、获取当前时间
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        //3、设置key
        String key = USER_SIGN_KEY + userId + ":" + keySuffix;
        //4、获取当前是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5、获取当前用户本月的签到数据
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                        .valueAt(0)
        );
        if(result == null || result.isEmpty()){
            return Result.ok(0);
        }
        //拿到十进制数字
        Long num = result.get(0);
        if(num == 0 || num == null){
            return Result.ok(0);
        }
        int count = 0;
        //6、循环遍历判断是否连续签到
        while(true){
            //将数与 1 做 与运算，得到签到数的最后一个bit位
            if((num & 1) == 0){
                //如果未签到，直接结束
                break;
            }else{
                //如果该天签到，计数器 + 1
                count++;
            }
            //将数右移一位,抛弃最后一个bit位，继续下一个bit位
            num = num >> 1;
        }
        //7、返回结果
        return Result.ok(count);
    }

    private User createUser(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX+RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
