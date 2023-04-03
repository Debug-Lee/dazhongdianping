package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Override
    public Result queryOneBlog(Long id) {
        //1、查询blog
        Blog blog = baseMapper.selectById(id);
        if(blog == null){
            return Result.fail("笔记不存在！");
        }
        //2、查询user信息
        selectUser(blog);
        //3、查询是否点赞
        isBlockLiked(blog);
        return Result.ok(blog);
    }


    @Override
    public Result queryHotBlogs(Integer current) {
        //根据博客点赞高低顺序分页查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            selectUser(blog);
            isBlockLiked(blog);
        });
        return Result.ok(records);
    }

    private void isBlockLiked(Blog blog) {
        //1、获取登录用户
        UserDTO user = UserHolder.getUser();
        if(user == null){
            return;
        }
        //2、判断当前用户是否已经点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        //zset  判断当前用户是否有权重，如果有则当前用户点赞了，如果没有那就没有点赞
        Double score = stringRedisTemplate.opsForZSet().score(key, user.getId().toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlogs(Long id) {
        //1、获取登录用户
        Long userId = UserHolder.getUser().getId();
        //2、判断当前用户是否已经点赞
        String key = BLOG_LIKED_KEY + id;
        // ZSCORE m1 key1 返回score，如果score不为空，则存在，返回空，则不不存在
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if(score == null){
            //3、未点赞
            //3.1 更新数据库  update tb_blog set liked = liked + 1 where id = ?
            boolean isUpdate = update().setSql("liked = liked + 1").eq("id", id).update();
            if(isUpdate){
                //3.2 向redis里添加当前用户
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else{
            //4、已点赞，取消点赞
            //4.1 减少数据库
            boolean isUpdate = update().setSql("liked = liked - 1").eq("id", id).update();
            //4.2 redis剔除当前用户
            if(isUpdate){
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id ;
        //1、查询top5点赞的用户  ZRANGE key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 5);
        if(top5 == null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //2、解析出其中的id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",",ids);
        //3、根据用户id查询出对于的用户，并封装到DTO
        //WHERE id IN (5 , 1) order by filed(id,5 , 1)
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //4、返回
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 1、获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2、保存探店博文
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("新增笔记失败！");
        }
        // 3、查询粉丝 select * from tb_follow where follow_user_id = ?
        List<Follow> lists = followService.query().eq("follow_user_id", user.getId()).list();
        // 4、推送流给粉丝
        for (Follow list : lists) {
            Long userId = list.getUserId();
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        // 5、返回笔记作者id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogofFollow(Long max, Integer offset) {
        //1、获取用户ID
        Long userId = UserHolder.getUser().getId();
        //2、获取当前用户收到的笔记流信息
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.
                opsForZSet().
                reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        //3、判断是否为空
        if(typedTuples.size() == 0 || typedTuples.isEmpty()){
            return Result.ok();
        }
        List<Long> ids = new ArrayList<>(typedTuples.size());   //直接指定list大小，避免反复扩容，优化速度
        long minTime = 0; //时间戳定义为long型
        int os = 1;
        //4、不为空，则进行解析
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            //4.1 获取id
            String value = tuple.getValue();
            ids.add(Long.valueOf(value));
            //4.2 获取分数
            long time = tuple.getScore().longValue();  //longValue 将double型变为long型
            if(minTime == time){    //如果有重复，则os+1
                os++;
            }else {
                minTime = time;     //如果没有重复，则更新最小时间戳作为下一次的max，并将offset置为1
                os = 1;
            }
        }

        //5、查询blog
        String idStr = StrUtil.join(",",ids);
        List<Blog> blogList = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        //5、1 查询是否点赞该笔记
        for (Blog blog : blogList) {
            //1、查询user信息
            selectUser(blog);
            //2、查询是否点赞
            isBlockLiked(blog);
        }
        //5、2 封装blog
        ScrollResult r = new ScrollResult();
        r.setList(blogList);
        r.setMinTime(minTime);
        r.setOffset(os);
        //6、返回数据
        return Result.ok(r);
    }

    /**
     * 根据userID查找user并存放在blog中
     * @param blog
     */
    private void selectUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
