package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long   LOGIN_CODE_TTL = 2L;

    public static final String LOGIN_TOKEN_KEY = "login:token:";
    public static final Long   LOGIN_TOKEN_TTL = 30L;

    public static final String CACHE_SHOP_KEY = "cache:shop:";
    public static final Long   CACHE_SHOP_NULL_TTL = 2L;
    public static final Long   CACHE_SHOP_TTL = 30L;

    public static final String LOCK_SHOP_TTL = "lock:shop";

    public static final String CACHE_SHOPLIST_KEY = "cache:shoplist:";

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";

    public static final String BLOG_LIKED_KEY = "blog:liked:";

    public static final String FEED_KEY = "feed:";

    public static final String SHOP_GEO_KEY = "shop:geo:";

    public static final String USER_SIGN_KEY = "sign:";

}
