package com.sl.transport.info.service.impl;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.util.ObjectUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.sl.transport.info.config.RedisConfig;
import com.sl.transport.info.domain.TransportInfoDTO;
import com.sl.transport.info.entity.TransportInfoDetail;
import com.sl.transport.info.entity.TransportInfoEntity;
import com.sl.transport.info.service.TransportInfoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @author analytics
 * @date 2024/11/4 16:18
 * @description
 */
@Slf4j
@Service
public class TransportInfoServiceImpl implements TransportInfoService {
    @Resource
    private MongoTemplate mongoTemplate;

    @Resource
    private Cache<String, TransportInfoDTO> transportInfoCache;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @CachePut(value = "transport-info", key = "#p0") //更新缓存数据
    @Override
    public TransportInfoEntity saveOrUpdate(String transportOrderId, TransportInfoDetail infoDetail) {
        // 根据运单id查询
        Query query = Query.query(Criteria.where("transportOrderId").is(transportOrderId)); //构造查询条件
        TransportInfoEntity transportInfoEntity = this.mongoTemplate.findOne(query, TransportInfoEntity.class);
        if (ObjectUtil.isNull(transportInfoEntity)) {
            // 运单信息不存在，新增数据
            transportInfoEntity = new TransportInfoEntity();
            transportInfoEntity.setTransportOrderId(transportOrderId);
            transportInfoEntity.setInfoList(ListUtil.toList(infoDetail));
            transportInfoEntity.setCreated(System.currentTimeMillis());
        } else {
            // 运单信息存在，只需要追加物流详情数据
            transportInfoEntity.getInfoList().add(infoDetail);
        }
        // 无论新增还是更新都要设置更新时间
        transportInfoEntity.setUpdated(System.currentTimeMillis());
        // 清除本地缓存中的数据
        // this.transportInfoCache.invalidate(transportOrderId);
        // Caffeine本地缓存一致性，发布订阅消息到redis，通知订阅者更新缓存
        this.stringRedisTemplate.convertAndSend(RedisConfig.CHANNEL_TOPIC, transportOrderId);
        // 保存/更新到MongoDB
        return this.mongoTemplate.save(transportInfoEntity);
    }

    @Cacheable(value = "transport-info", key = "#p0") //新增缓存数据
    @Override
    public TransportInfoEntity queryByTransportOrderId(String transportOrderId) {
        // 定义查询条件
        Query query = Query.query(Criteria.where("transportOrderId").is(transportOrderId));
        // 查询数据
        return this.mongoTemplate.findOne(query, TransportInfoEntity.class);
    }
}
