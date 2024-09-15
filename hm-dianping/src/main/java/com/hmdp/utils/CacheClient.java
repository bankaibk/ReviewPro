package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 查询缓存(解决缓存穿透)
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断命中缓存的是否是""(空值)，用于处理缓存穿透
        if (json != null) {
            return null;
        }
        // 4.不存在，查询数据库
        R res = dbFallback.apply(id);
        // 5.数据库不存在，返回错误
        if (res == null) {
            // 5.1 将""(空值)写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.数据库存在，写入redis
        this.set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(res), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 7.返回
        return res;
    }

    // 定义线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 逻辑过期解决缓存击穿
    public  <R, ID> R queryWithLogicExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isBlank(json)) {
            // 3.不存在，直接返回
            return null;
        }
        // 判断命中缓存的是否是""(空值)，用于处理缓存穿透
        // 逻辑过期主要解决热点key的缓存击穿问题，因此不考虑缓存穿透的现象
        // if (shopJson != null) {
        //     return null;
        // }
        // 4.命中，先把json数据反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        // 5.判断对象是否过期
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject data = (JSONObject) redisData.getData();
        R res = JSONUtil.toBean(data, type);
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1.未过期，直接返回店铺信息
            return res;
        }
        // 5.2.过期，缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        // 6.2.判断是否获取到锁
        if (tryLock(LOCK_SHOP_KEY + id)) {
            // 获取锁成功，当二次判断缓存是否已经更新
            json = stringRedisTemplate.opsForValue().get(key);
            redisData = JSONUtil.toBean(json, RedisData.class);
            expireTime = redisData.getExpireTime();
            data = (JSONObject) redisData.getData();
            res = JSONUtil.toBean(data, type);
            if (expireTime.isAfter(LocalDateTime.now())) {
                return res;
            }
            // TODO 6.3.获取成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r = dbFallback.apply(id);
                    this.setWithLogicExpire(key, r, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(LOCK_SHOP_KEY + id);
                }
            });
        }
        // 7.返回
        return res;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}
