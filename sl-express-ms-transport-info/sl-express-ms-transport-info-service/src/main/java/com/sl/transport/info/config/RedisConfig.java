package com.sl.transport.info.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.cache.RedisCacheWriter;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

/**
 * Redis相关的配置
 */
@Configuration
public class RedisConfig {

    /**
     * 存储的默认有效期时间，单位：小时
     */
    @Value("${redis.ttl:1}")
    private Integer redisTtl;

    public static final String CHANNEL_TOPIC = "sl-express-ms-transport-info-caffeine";

    @Bean
    public RedisCacheManager redisCacheManager(RedisTemplate redisTemplate) {
        // 默认配置
        RedisCacheConfiguration defaultCacheConfiguration = RedisCacheConfiguration.defaultCacheConfig()
                // 设置key的序列化方式为字符串
                .serializeKeysWith(RedisSerializationContext.SerializationPair.fromSerializer(new StringRedisSerializer()))
                // 设置value的序列化方式为json格式
                .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(new GenericJackson2JsonRedisSerializer()))
                .disableCachingNullValues() // 不缓存null
                .entryTtl(Duration.ofHours(redisTtl));  // 默认缓存数据保存1小时

        // 构redis缓存管理器
        // RedisCacheManager redisCacheManager = RedisCacheManager.RedisCacheManagerBuilder
        //         .fromConnectionFactory(redisTemplate.getConnectionFactory())
        //         .cacheDefaults(defaultCacheConfiguration)
        //         .transactionAware() // 只在事务成功提交后才会进行缓存的put/evict操作
        //         .build();
        // 使用自定义缓存管理器
        RedisCacheWriter redisCacheWriter = RedisCacheWriter.nonLockingRedisCacheWriter(redisTemplate.getConnectionFactory());
        MyRedisCacheManager myRedisCacheManager = new MyRedisCacheManager(redisCacheWriter, defaultCacheConfiguration);
        myRedisCacheManager.setTransactionAware(true); // 只在事务成功提交后才会进行缓存的put/evict操作
        return myRedisCacheManager;
    }


    /**
     * 配置订阅，用于解决Caffeine一致性的问题
     *
     * @param connectionFactory 链接工厂
     * @param listenerAdapter 消息监听器
     * @return 消息监听容器
     */
    @Bean
    public RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory,
                                                   MessageListenerAdapter listenerAdapter) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(listenerAdapter, new ChannelTopic(CHANNEL_TOPIC));
        return container;
    }
}