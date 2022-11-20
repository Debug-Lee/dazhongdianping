--1.参数列表
--1.1 优惠券id
    local voucherId = ARGV[1]
--1.2 用户id
    local userId = ARGV[2]
--1.3 订单id
    local orderId = ARGV[3]

--2.数据库key
--2.2 库存key
    local stockKey = "seckill:stock:" .. voucherId
--2.3 某个优惠券下单用户的key
    local orderKey = "seckill:order:" .. voucherId

--3.业务内容
--3.1查看库存是否充足 get stockKey
    if(tonumber(redis.call("get",stockKey)) <= 0) then
        --如果库存不充足，返回1
        return 1
    end
--3.2一人一单   sisnumber orderKey userId
    if(redis.call('sismember',orderKey,userId) == 1) then
        --如果已经买过，返回2
        return 2
    end
--3.3 扣库存 incrby stockKey -1
    redis.call("incrby",stockKey,-1)
--3.4 下单，保存用户 sadd orderKey userId
    redis.call("sadd",orderKey,userId)
--3.5 将信息存入消息队列 XADD stream.orders * k1 v1 k2 v2 k3 v3
    redis.call("XADD","stream.orders","*","voucherId",voucherId,"userId",userId,"id",orderId)
--具有秒杀资格，返回0
return 0