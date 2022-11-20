--比较是否相等
if(redis.call("get",KEYS[1]) == ARGV[1]) then
    --一致，则删除锁
    return redis.call("del",KEYS[1])
end

--不一致，释放失败
return 0