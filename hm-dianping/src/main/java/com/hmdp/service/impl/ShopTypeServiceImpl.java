package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

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
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryList() {
        // 1.在redis中查询店铺类型
        Long size = stringRedisTemplate.opsForList().size(CACHE_SHOP_TYPE_KEY);
        if (size != 0) {
            log.debug("11111111");
            List<String> list = stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE_KEY, 0, size - 1);
            ArrayList<ShopType> shopTypesCache = new ArrayList<>();
            for (String shopType : list) {
                ShopType type = JSONUtil.toBean(shopType, ShopType.class);
                shopTypesCache.add(type);
            }
            return Result.ok(shopTypesCache);
        }
        // 2.redis未命中，查询sql
        List<ShopType> shopTypesSql = list();
        for (ShopType shopType : shopTypesSql) {
            String str = JSONUtil.toJsonStr(shopType);
            stringRedisTemplate.opsForList().rightPush(CACHE_SHOP_TYPE_KEY, str);
        }
        return Result.ok(shopTypesSql);
    }
}
