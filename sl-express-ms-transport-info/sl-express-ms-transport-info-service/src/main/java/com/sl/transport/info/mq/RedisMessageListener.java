package com.sl.transport.info.mq;

import cn.hutool.core.convert.Convert;
import com.github.benmanes.caffeine.cache.Cache;
import com.sl.transport.info.domain.TransportInfoDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * redis消息监听，解决Caffeine一致性的问题
 */
@Slf4j
@Component
public class RedisMessageListener extends MessageListenerAdapter {

    @Resource
    private Cache<String, TransportInfoDTO> transportInfoCache;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        // 获取到消息中的运单id
        String transportOrderId = Convert.toStr(message);
        log.info("redis消息监听缓存变更，运单id:{}", transportOrderId);
        // 将本jvm中的缓存删除掉
        this.transportInfoCache.invalidate(transportOrderId);
    }
}
