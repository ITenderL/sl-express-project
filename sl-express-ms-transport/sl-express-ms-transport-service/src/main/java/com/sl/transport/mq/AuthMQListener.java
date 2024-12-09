package com.sl.transport.mq;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.sl.transport.common.constant.Constants;
import com.sl.transport.entity.node.AgencyEntity;
import com.sl.transport.entity.node.BaseEntity;
import com.sl.transport.entity.node.OLTEntity;
import com.sl.transport.entity.node.TLTEntity;
import com.sl.transport.enums.OrganTypeEnum;
import com.sl.transport.service.IService;
import com.sl.transport.utils.OrganServiceFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * @author analytics
 * @date 2024/10/22 22:25
 * @description
 */
@Slf4j
@Component
public class AuthMQListener {

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = Constants.MQ.Queues.AUTH_TRANSPORT),
            exchange = @Exchange(name = "${rabbitmq.exchange}", type = ExchangeTypes.TOPIC),
            key = "#"
    ))
    public void listenAgencyMsg(String msg) {
        log.info("AuthMQListener收到消息:{}", msg);
        JSONObject jsonObject = JSONUtil.parseObj(msg);
        String type = jsonObject.getStr("type");
        if (!CharSequenceUtil.equalsIgnoreCase(type, "ORG")) {
            return;
        }
        String operation = jsonObject.getStr("operation");
        JSONObject content = (JSONObject) jsonObject.getJSONArray("content").getObj(0);
        String name = content.getStr("name");
        Long parentId = content.getLong("parentId");
        IService iservice;
        BaseEntity entity;
        if (CharSequenceUtil.endWith(name, "转运中心")) {
            iservice = OrganServiceFactory.getBean(OrganTypeEnum.OLT.getCode());
            entity = new OLTEntity();
        } else if (CharSequenceUtil.endWith(name, "分拣中心")) {
            iservice = OrganServiceFactory.getBean(OrganTypeEnum.TLT.getCode());
            entity = new TLTEntity();
        } else if (CharSequenceUtil.endWith(name, "营业部")) {
            iservice = OrganServiceFactory.getBean(OrganTypeEnum.AGENCY.getCode());
            entity = new AgencyEntity();
        } else {
            return;
        }
        // 设置参数
        entity.setBid(content.getLong("id"));
        entity.setName(name);
        entity.setStatus(content.getBool("status"));
        entity.setParentId(parentId);
        switch (operation) {
            case "ADD":
                iservice.create(entity);
                break;
            case "UPDATE":
                iservice.update(entity);
                break;
            case "DEL":
                iservice.deleteByBid(entity.getBid());
                break;
            default:
                break;
        }
    }
}
