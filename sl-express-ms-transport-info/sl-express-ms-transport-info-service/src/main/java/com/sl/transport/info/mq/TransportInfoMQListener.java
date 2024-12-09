package com.sl.transport.info.mq;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.sl.ms.transport.api.OrganFeign;
import com.sl.transport.common.constant.Constants;
import com.sl.transport.common.vo.TransportInfoMsg;
import com.sl.transport.domain.OrganDTO;
import com.sl.transport.info.entity.TransportInfoDetail;
import com.sl.transport.info.service.TransportInfoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

/**
 * 物流信息消息
 */
@Slf4j
@Component
public class TransportInfoMQListener {

    @Resource
    private OrganFeign organFeign;

    @Resource
    private TransportInfoService transportInfoService;

    /**
     * 监听物流信息消息
     *
     * @param msg
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = Constants.MQ.Queues.TRANSPORT_INFO_APPEND),
            exchange = @Exchange(name = Constants.MQ.Exchanges.TRANSPORT_INFO, type = ExchangeTypes.TOPIC),
            key = Constants.MQ.RoutingKeys.TRANSPORT_INFO_APPEND
    ))
    @Transactional(rollbackFor = Exception.class)
    public void listenTransportInfoMsg(String msg) {
        // {"info":"您的快件已到达【$organId】", "status":"运输中", "organId":90001, "transportOrderId":"SL920733749248" , "created":1653133234913}
        log.info("TransportInfoMQListener.listenTransportInfoMsg:{}", msg);
        TransportInfoMsg transportInfoMsg = JSONUtil.toBean(msg, TransportInfoMsg.class);
        String info = transportInfoMsg.getInfo();
        // 替换消息中的机构名称
        if (CharSequenceUtil.contains(info, "$organId")) {
            OrganDTO organDTO = organFeign.queryById(transportInfoMsg.getOrganId());
            info = StrUtil.replace(info, "$organId", organDTO.getName());
        }
        // 构建TransportInfoDetail
        TransportInfoDetail transportInfoDetail = TransportInfoDetail.builder()
                .info(info)
                .status(transportInfoMsg.getStatus())
                .created(transportInfoMsg.getCreated())
                .build();
        // 保存数据到Mongo
        transportInfoService.saveOrUpdate(transportInfoMsg.getTransportOrderId(), transportInfoDetail);
    }
}
