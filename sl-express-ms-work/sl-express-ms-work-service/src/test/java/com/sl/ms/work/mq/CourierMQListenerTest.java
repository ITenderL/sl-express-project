package com.sl.ms.work.mq;

import cn.hutool.json.JSONUtil;
import com.sl.transport.common.vo.CourierMsg;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
class CourierMQListenerTest {

    @Resource
    private CourierMQListener courierMQListener;

    @Test
    void listenCourierTaskMsg() {
    }

    @Test
    void listenCourierPickupMsg() {
        CourierMsg courierMsg = new CourierMsg();
        //目前只用到订单id
        courierMsg.setOrderId(1590586236289646594L);
        String msg = JSONUtil.toJsonStr(courierMsg);
        this.courierMQListener.listenCourierPickupMsg(msg);
    }
}