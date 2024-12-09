package com.sl.ms.dispatch.mq;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.json.JSONUtil;
import com.sl.ms.api.CourierFeign;
import com.sl.ms.base.api.common.MQFeign;
import com.sl.ms.work.api.PickupDispatchTaskFeign;
import com.sl.ms.work.domain.dto.CourierTaskCountDTO;
import com.sl.ms.work.domain.enums.pickupDispatchtask.PickupDispatchTaskType;
import com.sl.transport.common.constant.Constants;
import com.sl.transport.common.util.ObjectUtil;
import com.sl.transport.common.vo.CourierTaskMsg;
import com.sl.transport.common.vo.OrderMsg;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 订单业务消息，接收到新订单后，根据快递员的负载情况，分配快递员
 */
@Slf4j
@Component
public class OrderMQListener {

    @Resource
    private CourierFeign courierFeign;
    @Resource
    private PickupDispatchTaskFeign pickupDispatchTaskFeign;
    @Resource
    private MQFeign mqFeign;

    /**
     * 如果有多个快递员，需要查询快递员今日的取派件数，根据此数量进行计算
     * 计算的逻辑：优先分配取件任务少的，取件数相同的取第一个分配
     * <p>
     * 发送生成取件任务时需要计算时间差，如果小于2小时，实时发送；大于2小时，延时发送
     * 举例：
     * 1、现在10:30分，用户期望：11:00 ~ 12:00上门，实时发送
     * 2、现在10:30分，用户期望：13:00 ~ 14:00上门，延时发送，12点发送消息，延时1.5小时发送
     *
     * @param msg 消息内容
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = Constants.MQ.Queues.DISPATCH_ORDER_TO_PICKUP_DISPATCH_TASK),
            exchange = @Exchange(name = Constants.MQ.Exchanges.ORDER_DELAYED, type = ExchangeTypes.TOPIC, delayed = Constants.MQ.DELAYED),
            key = Constants.MQ.RoutingKeys.ORDER_CREATE
    ))
    public void listenOrderMsg(String msg) {
        // {"orderId":123, "agencyId": 8001, "taskType":1, "mark":"带包装", "longitude":116.111, "latitude":39.00, "created":1654224658728, "estimatedStartTime": 1654224658728}
        log.info("接收到订单的消息 >>> msg = {}", msg);
        OrderMsg orderMsg = JSONUtil.toBean(msg, OrderMsg.class);
        Long agencyId = orderMsg.getAgencyId();
        Double latitude = orderMsg.getLatitude();
        Double longitude = orderMsg.getLongitude();
        List<Long> courierIds = courierFeign.queryCourierIdListByCondition(agencyId, longitude, latitude, LocalDateTimeUtil.toEpochMilli(orderMsg.getEstimatedEndTime()));
        Long selectedCourierId = null;
        if (CollUtil.isNotEmpty(courierIds)) {
            selectedCourierId = selectCourier(courierIds, orderMsg.getTaskType());
            log.info("根据当日任务选出的快递员id：{}", selectedCourierId);
        }
        // 发送消息
        CourierTaskMsg courierTaskMsg = CourierTaskMsg.builder()
                .courierId(selectedCourierId)
                .agencyId(agencyId)
                .taskType(orderMsg.getTaskType())
                .orderId(orderMsg.getOrderId())
                .mark(orderMsg.getMark())
                .estimatedEndTime(orderMsg.getEstimatedEndTime())
                .created(System.currentTimeMillis())
                .build();
        // 计算时间差
        long between = LocalDateTimeUtil.between(LocalDateTime.now(), orderMsg.getEstimatedEndTime(), ChronoUnit.MINUTES);
        // 默认实时发送
        int delay = Constants.MQ.DEFAULT_DELAY;
        if (between > 120 && ObjectUtil.equal(orderMsg.getTaskType(), 1)) {
            // 计算延时时间，单位毫秒
            LocalDateTime sendDataTime = LocalDateTimeUtil.offset(orderMsg.getEstimatedEndTime(), -2, ChronoUnit.HOURS);
            delay = Convert.toInt(LocalDateTimeUtil.between(LocalDateTime.now(), sendDataTime, ChronoUnit.MILLIS));
        }
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.PICKUP_DISPATCH_TASK_DELAYED,
                Constants.MQ.RoutingKeys.PICKUP_DISPATCH_TASK_CREATE, courierTaskMsg.toJson(), delay);
    }

    /**
     * 选择快递员
     *
     * @param courierIds
     * @param taskType
     * @return
     */
    private Long selectCourier(List<Long> courierIds, Integer taskType) {
        // 根据快递员的负载情况，选择合适的快递员
        // 如果只有一个快递员，直接分配
        if (courierIds.size() == 1) {
            return courierIds.get(0);
        }
        // 如果有多个快递员，需要查询快递员今日的取派件数，根据此数量进行计算
        String dateStr = DateUtil.date().toDateStr();
        // 查询快递员今日的取派件数，根据此数量进行计算
        List<CourierTaskCountDTO> countDTOS = pickupDispatchTaskFeign.findCountByCourierIds(courierIds, PickupDispatchTaskType.codeOf(taskType), dateStr);
        // 返回的任务书是否于快递员的数量相等
        if (countDTOS.size() != courierIds.size()) {
            List<CourierTaskCountDTO> dtoList = courierIds.stream()
                    .filter(courierId -> CollUtil.indexOf(countDTOS, dto -> ObjectUtil.equal(dto.getCourierId(), courierId)) == -1)
                    .map(courierId -> CourierTaskCountDTO.builder()
                            .courierId(courierId)
                            .count(0L).build())
                    .collect(Collectors.toList());
            // 补齐到集合中
            countDTOS.addAll(dtoList);
        }
        // 选中任务数最小的快递员进行分配
        CollUtil.sortByProperty(countDTOS, "count");
        return countDTOS.get(0).getCourierId();
    }
}
