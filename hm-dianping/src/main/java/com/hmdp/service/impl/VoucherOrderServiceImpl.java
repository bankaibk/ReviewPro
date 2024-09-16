package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.aop.framework.DefaultAdvisorChainFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                // 获取订单信息
                try {
                    // 获取消息队列中订单
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    if (list == null || list.isEmpty()) {
                        // 获取失败，进入下一次循环
                        continue;
                    }
                    // 获取成功，创建订单
                    MapRecord<String, Object, Object> mapRecord = list.get(0);
                    Map<Object, Object> value = mapRecord.getValue();
                    VoucherOrder order = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(
                            queueName, "g1", mapRecord.getId()
                    );
                    handlerVoucherOrder(order);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlerPendingList();
                }
            }
        }

        private void handlerPendingList() {
            while (true) {
                // 获取订单信息
                try {
                    // 获取消息队列中订单
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    if (list == null || list.isEmpty()) {
                        // 获取失败，pending-list中没有消息，结束
                        break;
                    }
                    // 获取成功，创建订单
                    MapRecord<String, Object, Object> mapRecord = list.get(0);
                    Map<Object, Object> value = mapRecord.getValue();
                    VoucherOrder order = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(
                            queueName, "g1", mapRecord.getId()
                    );
                    handlerVoucherOrder(order);
                } catch (Exception e) {
                    log.error("处理pending-list异常信息", e);
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }


    // 调用线程不断循环，处理阻塞队列中的订单信息
    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                // 获取订单信息
                try {
                    VoucherOrder order = orderTasks.take();
                    // 创建订单
                    handlerVoucherOrder(order);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    private void handlerVoucherOrder(VoucherOrder order) {
        Long userId = order.getUserId();
        // 使用redisson创建锁
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (isLock) {
            // 获取失败
            log.error("下单失败");
            return;
        }
        try {
            // 获取成功，获取代理对象(确保事务)
            proxy.createVoucherOrder(order);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    // 消息队列实现异步下单
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 执行lua脚本
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        int result = stringRedisTemplate
                .execute(SECKILL_SCRIPT,
                        Collections.emptyList(),
                        voucherId.toString(), userId.toString(), String.valueOf(orderId)).intValue();
        // 判断结果，是否有购买资格
        if (result != 0)
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    //阻塞队列实现异步下单
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 执行lua脚本
        Long userId = UserHolder.getUser().getId();
        int result = stringRedisTemplate
                .execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString()).intValue();
        // 判断结果，是否有购买资格
        if (result != 0)
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        // TODO 有资格，将下单信息保存至队列
        // 创建订单
        VoucherOrder order = new VoucherOrder();
        // 订单id
        long orderId = redisIdWorker.nextId("order");
        order.setId(orderId);
        // 用户id
        order.setUserId(userId);
        // 代金券id
        order.setVoucherId(voucherId);
        // 放入阻塞队列
        orderTasks.add(order);
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }*/

    // 分布式锁实现秒杀下单，一人一单
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2.判断秒杀活动是否有效
        if (voucher.getBeginTime().isAfter(LocalDateTime.now()))
            return Result.fail("秒杀尚未开始");
        if (voucher.getEndTime().isBefore(LocalDateTime.now()))
            return Result.fail("秒杀已经结束");
        // 3.判断库存是否充足
        if (voucher.getStock() < 1)
            return Result.fail("库存不足");
        Long userId = UserHolder.getUser().getId();
        // JVM互斥锁机制实现
        *//*synchronized (userId.toString().intern()) {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }*//*
        // 分布式锁解决一人一单以及超卖
        // 创建锁
        *//*SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);*//*
        // 使用redisson创建锁
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            // 获取失败
            return Result.fail("您已购买一次");
        }
        try {
            // 获取成功，获取代理对象(确保事务)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }*/

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 一人一单
        Long userId = UserHolder.getUser().getId();
        // 用户id的值一样时，就被锁住
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            // 该用户已经购买
            return Result.fail("您已购买一次");
        }
        // 4.扣减库存(乐观锁)
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0).update();
        if (!success)
            return Result.fail("库存不足");
        // 5.创建订单
        VoucherOrder order = new VoucherOrder();
        // 5.1.订单id
        long orderId = redisIdWorker.nextId("order");
        order.setId(orderId);
        // 5.2.用户id
        order.setUserId(userId);
        // 5.3.代金券id
        order.setVoucherId(voucherId);
        save(order);
        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder order) {
        // 一人一单
        Long userId = order.getUserId();
        // 用户id的值一样时，就被锁住
        int count = query().eq("user_id", userId).eq("voucher_id", order.getVoucherId()).count();
        if (count > 0) {
            // 该用户已经购买
            log.error("下单失败");
        }
        // 4.扣减库存(乐观锁)
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", order.getVoucherId())
                .gt("stock", 0).update();
        if (!success)
            log.error("下单失败");
        // 保存订单
        save(order);
    }
}
