package com.sl.ms.dispatch.job;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import com.google.common.collect.Lists;
import com.sl.ms.base.api.common.MQFeign;
import com.sl.ms.base.api.truck.TruckPlanFeign;
import com.sl.ms.base.domain.truck.TruckDto;
import com.sl.ms.base.domain.truck.TruckPlanDto;
import com.sl.ms.dispatch.dto.DispatchMsgDTO;
import com.sl.ms.dispatch.mq.TransportOrderDispatchMQListener;
import com.sl.transport.common.constant.Constants;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 调度运输任务
 */
@Component
@Slf4j
public class DispatchJob {

    @Resource
    private TruckPlanFeign truckPlanFeign;

    @Resource
    private TransportOrderDispatchMQListener transportOrderDispatchMQListener;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private MQFeign mqFeign;

    @Value("${sl.volume.ratio:0.95}")
    private Double volumeRatio;

    @Value("${sl.weight.ratio:0.95}")
    private Double weightRatio;

    /**
     * 分片广播方式处理运单，生成运输任务
     */
    @XxlJob("transportTask")
    public void transportTask() {
        // 分片参数
        int shardIndex = XxlJobHelper.getShardIndex();
        int shardTotal = XxlJobHelper.getShardTotal();
        // 根据分片参数，查询2小时内并且可用的车辆
        List<TruckPlanDto> truckPlanDtos = truckPlanFeign.pullUnassignedPlan(shardTotal, shardIndex);
        if (CollUtil.isEmpty(truckPlanDtos)) {
            return;
        }
        // 遍历车辆，生成运输任务
        for (TruckPlanDto truckPlanDto : truckPlanDtos) {
            //校验车辆计划对象
            if (ObjectUtil.hasEmpty(truckPlanDto.getStartOrganId(), truckPlanDto.getEndOrganId(),
                    truckPlanDto.getTransportTripsId(), truckPlanDto.getId())) {
                log.error("车辆计划对象数据不符合要求， truckPlanDto -> {}", truckPlanDto);
                continue;
            }
            Long startOrganId = truckPlanDto.getStartOrganId();
            Long endOrganId = truckPlanDto.getEndOrganId();
            String redisKey = transportOrderDispatchMQListener.getListRedisKey(startOrganId, endOrganId);
            // 设置redis锁的key
            String lockRedisKey = Constants.LOCKS.DISPATCH_LOCK_PREFIX + redisKey;
            // 获取锁，采用公平锁，如果没有获取到锁，则一直等待，直到获取到锁为止
            RLock lock = redissonClient.getFairLock(lockRedisKey);
            List<DispatchMsgDTO> dispatchMsgDTOList = Lists.newArrayList();
            try {
                // 锁定，一直等待锁，一定要获取到锁，因为查询到车辆的调度状态设置为：已分配
                lock.lock();
                // 计算车辆运力、合并运单
                executeTransportTask(redisKey, truckPlanDto.getTruckDto(), dispatchMsgDTOList);
            } finally {
                // 释放锁
                lock.unlock();
            }
            // 生成运输任务
            createTransportTask(truckPlanDto, startOrganId, endOrganId, dispatchMsgDTOList);
        }
        // 发送消息通过车辆已经完成调度
        completeTruckPlan(truckPlanDtos);
    }

    /**
     * 发送消息通过车辆已经完成调度
     *
     * @param truckPlanDtos
     */
    private void completeTruckPlan(List<TruckPlanDto> truckPlanDtos) {
        // {"ids":[1,2,3], "created":123456}
        Map<String, Object> msg = MapUtil.<String, Object>builder()
                .put("ids", truckPlanDtos.stream().map(TruckPlanDto::getId).collect(Collectors.toList()))
                .put("created", System.currentTimeMillis()).build();
        String jsonMsg = JSONUtil.toJsonStr(msg);
        // 发送消息
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRUCK_PLAN,
                Constants.MQ.RoutingKeys.TRUCK_PLAN_COMPLETE, jsonMsg);
    }

    /**
     * 生成运输任务
     *
     * @param truckPlanDto
     * @param startOrganId
     * @param endOrganId
     * @param dispatchMsgDTOList
     */
    private void createTransportTask(TruckPlanDto truckPlanDto, Long startOrganId, Long endOrganId, List<DispatchMsgDTO> dispatchMsgDTOList) {
        // 将运单合并的结果以消息的方式发送出去
        // key-> 车辆id，value ->  运单id列表
        // {"driverId":123, "truckPlanId":456, "truckId":1210114964812075008,"totalVolume":4.2,"endOrganId":90001,"totalWeight":7,"transportOrderIdList":[320733749248,420733749248],"startOrganId":100280}
        List<String> transportOrderIdList = dispatchMsgDTOList.stream().map(DispatchMsgDTO::getTransportOrderId).collect(Collectors.toList());
        // 司机列表确保不为null
        List<Long> driverIds = CollUtil.isNotEmpty(truckPlanDto.getDriverIds()) ? truckPlanDto.getDriverIds() : ListUtil.empty();
        Map<String, Object> msgResult = MapUtil.<String, Object>builder()
                .put("truckId", truckPlanDto.getTruckId()) //车辆id
                .put("driverIds", driverIds) //司机id
                .put("truckPlanId", truckPlanDto.getId()) //车辆计划id
                .put("transportTripsId", truckPlanDto.getTransportTripsId()) //车次id
                .put("startOrganId", startOrganId) //开始机构id
                .put("endOrganId", endOrganId) //结束机构id
                // 运单id列表
                .put("transportOrderIdList", transportOrderIdList)
                // 总重量
                .put("totalWeight", dispatchMsgDTOList.stream()
                        .mapToDouble(DispatchMsgDTO::getTotalWeight)
                        .sum())
                // 总体积
                .put("totalVolume", dispatchMsgDTOList.stream()
                        .mapToDouble(DispatchMsgDTO::getTotalVolume)
                        .sum())
                .build();
        // 发送消息
        String jsonMsg = JSONUtil.toJsonStr(msgResult);
        mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_TASK, Constants.MQ.RoutingKeys.TRANSPORT_TASK_CREATE, jsonMsg);
        if (CollUtil.isNotEmpty(transportOrderIdList)) {
            // 删除redis中set存储的运单数据
            String setRedisKey = transportOrderDispatchMQListener.getSetRedisKey(startOrganId, endOrganId);
            stringRedisTemplate.opsForSet().remove(setRedisKey, transportOrderIdList.toArray());
        }
    }

    /**
     * 计算该车辆已经分配的运单，是否超出其运力，载重 或 体积超出，需要将新拿到的运单加进去后进行比较
     *
     * @param redisKey
     * @param truckDto
     * @param dispatchMsgDTOList
     */
    private void executeTransportTask(String redisKey, TruckDto truckDto, List<DispatchMsgDTO> dispatchMsgDTOList) {
        String redisData = stringRedisTemplate.opsForList().rightPop(redisKey);
        if (CharSequenceUtil.isEmpty(redisData)) {
            // 该车辆没有运单需要运输
            return;
        }
        DispatchMsgDTO dispatchMsgDTO = JSONUtil.toBean(redisData, DispatchMsgDTO.class);
        // 计算该车辆已经分配的运单，是否超出其运力，载重 或 体积超出，需要将新拿到的运单加进去后进行比较
        BigDecimal totalWeight = NumberUtil.add(NumberUtil.toBigDecimal(dispatchMsgDTOList.stream()
                .mapToDouble(DispatchMsgDTO::getTotalWeight)
                .sum()), dispatchMsgDTO.getTotalWeight());
        BigDecimal totalVolume = NumberUtil.add(NumberUtil.toBigDecimal(dispatchMsgDTOList.stream()
                .mapToDouble(DispatchMsgDTO::getTotalVolume)
                .sum()), dispatchMsgDTO.getTotalVolume());
        // 车辆最大的容积和载重要留有余量，否则可能会超重 或 装不下
        BigDecimal maxAllowableLoad = NumberUtil.mul(truckDto.getAllowableLoad(), weightRatio);
        BigDecimal maxAllowableVolume = NumberUtil.mul(truckDto.getAllowableVolume(), volumeRatio);
        if (NumberUtil.isGreaterOrEqual(totalWeight, maxAllowableLoad)
                || NumberUtil.isGreaterOrEqual(totalVolume, maxAllowableVolume)) {
            // 超出车辆运力，需要取货的运单再放回去，放到最右边，以便保证运单处理的顺序
            stringRedisTemplate.opsForList().rightPush(redisKey, redisData);
            return;
        }
        // 没有超出运力，将该运单加入到集合中
        dispatchMsgDTOList.add(dispatchMsgDTO);
        // 递归处理运单
        executeTransportTask(redisKey, truckDto, dispatchMsgDTOList);
    }
}

