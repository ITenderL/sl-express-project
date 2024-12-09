package com.sl.ms.work.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.sl.ms.base.api.common.MQFeign;
import com.sl.ms.oms.api.CargoFeign;
import com.sl.ms.oms.api.OrderFeign;
import com.sl.ms.oms.dto.OrderCargoDTO;
import com.sl.ms.oms.dto.OrderDTO;
import com.sl.ms.oms.dto.OrderLocationDTO;
import com.sl.ms.transport.api.OrganFeign;
import com.sl.ms.transport.api.TransportLineFeign;
import com.sl.ms.work.domain.dto.TransportOrderDTO;
import com.sl.ms.work.domain.dto.request.TransportOrderQueryDTO;
import com.sl.ms.work.domain.dto.response.TransportOrderStatusCountDTO;
import com.sl.ms.work.domain.enums.WorkExceptionEnum;
import com.sl.ms.work.domain.enums.pickupDispatchtask.PickupDispatchTaskType;
import com.sl.ms.work.domain.enums.transportorder.TransportOrderSchedulingStatus;
import com.sl.ms.work.domain.enums.transportorder.TransportOrderStatus;
import com.sl.ms.work.entity.TransportOrderEntity;
import com.sl.ms.work.entity.TransportOrderTaskEntity;
import com.sl.ms.work.mapper.TransportOrderMapper;
import com.sl.ms.work.mapper.TransportOrderTaskMapper;
import com.sl.ms.work.service.TransportOrderService;
import com.sl.ms.work.service.TransportTaskService;
import com.sl.transport.common.constant.Constants;
import com.sl.transport.common.enums.IdEnum;
import com.sl.transport.common.exception.SLException;
import com.sl.transport.common.service.IdService;
import com.sl.transport.common.util.PageResponse;
import com.sl.transport.common.vo.OrderMsg;
import com.sl.transport.common.vo.TransportInfoMsg;
import com.sl.transport.common.vo.TransportOrderMsg;
import com.sl.transport.common.vo.TransportOrderStatusMsg;
import com.sl.transport.domain.OrganDTO;
import com.sl.transport.domain.TransportLineNodeDTO;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author analytics
 * @date 2024/10/24 22:00
 * @description
 */
@Service
public class TransportOrderServiceImpl extends ServiceImpl<TransportOrderMapper, TransportOrderEntity> implements TransportOrderService {

    @Resource
    private OrderFeign orderFeign;

    @Resource
    private CargoFeign cargoFeign;

    @Resource
    private TransportLineFeign transportLineFeign;

    @Resource
    private IdService idService;

    @Resource
    private MQFeign mqFeign;

    @Resource
    private OrganFeign organFeign;

    @Resource
    private TransportOrderTaskMapper transportOrderTaskMapper;

    @Resource
    private TransportTaskService transportTaskService;

    @Transactional(rollbackFor = Exception.class)
    @Override
    public TransportOrderEntity orderToTransportOrder(Long orderId) {
        // 幂等校验，根据订单id查询关联的运输单是否存在，存在直接返回
        TransportOrderEntity transportOrderEntity = findByOrderId(orderId);
        if (ObjectUtil.isNotEmpty(transportOrderEntity)) {
            throw new SLException(WorkExceptionEnum.ORDER_NOT_FOUND);
        }
        // 根据订单id，查询订单货物信息
        OrderCargoDTO orderCargoDTO = cargoFeign.findByOrderId(orderId);
        if (ObjectUtil.isEmpty(orderCargoDTO)) {
            throw new SLException(WorkExceptionEnum.ORDER_CARGO_NOT_FOUND);
        }
        // 根据订单id，查询订单信息
        OrderDTO orderDTO = orderFeign.findById(orderId);
        if (ObjectUtil.isNull(orderDTO)) {
            throw new SLException(WorkExceptionEnum.ORDER_NOT_FOUND);
        }
        // 根据订单id，查询订单位置信息
        OrderLocationDTO orderLocationDTO = orderFeign.findOrderLocationByOrderId(orderId);
        if (ObjectUtil.isNull(orderLocationDTO)) {
            throw new SLException(WorkExceptionEnum.ORDER_LOCATION_NOT_FOUND);
        }
        // 起始网点id
        Long sendAgentId = Convert.toLong(orderLocationDTO.getSendAgentId());
        Long receiveAgentId = Convert.toLong(orderLocationDTO.getReceiveAgentId());
        // 是否参与调度
        // TransportLineNodeDTO transportLineNodeDTO = null;
        // boolean isDispatch = true;
        // // 判断起始网点id和终点网点id是否相同，如果相同，则不参与调度
        // if (ObjectUtil.equal(sendAgentId, receiveAgentId)) {
        //     isDispatch = false;
        // } else {
        //     // 根据调度配置，查询路线信息
        //     transportLineNodeDTO = transportLineFeign.queryPathByDispatchMethod(sendAgentId, receiveAgentId);
        //     if (ObjectUtil.isEmpty(transportLineNodeDTO) || CollUtil.isEmpty(transportLineNodeDTO.getNodeList())) {
        //         throw new SLException(WorkExceptionEnum.TRANSPORT_LINE_NOT_FOUND);
        //     }
        // }
        // 构建运输单实体类
        transportOrderEntity = new TransportOrderEntity();
        // 设置id
        transportOrderEntity.setId(this.idService.getId(IdEnum.TRANSPORT_ORDER));
        // 订单ID
        transportOrderEntity.setOrderId(orderId);
        // 起始网点id
        transportOrderEntity.setStartAgencyId(sendAgentId);
        // 终点网点id
        transportOrderEntity.setEndAgencyId(receiveAgentId);
        // 当前所在机构id
        transportOrderEntity.setCurrentAgencyId(sendAgentId);
        // 货品总体积，单位m^3
        transportOrderEntity.setTotalVolume(orderCargoDTO.getVolume());
        // 货品总重量，单位kg
        transportOrderEntity.setTotalWeight(orderCargoDTO.getWeight());
        // 默认非拒收订单
        transportOrderEntity.setIsRejection(false);
        // 起始网点名称是否相等，如果相等则不需要调度，否则需要调度
        boolean isDispatch = !ObjectUtil.equal(sendAgentId, receiveAgentId);
        if (!isDispatch) {
            transportOrderEntity.setNextAgencyId(receiveAgentId);
            transportOrderEntity.setStatus(TransportOrderStatus.ARRIVED_END);
            transportOrderEntity.setSchedulingStatus(TransportOrderSchedulingStatus.SCHEDULED);
        } else {
            transportOrderEntity.setStatus(TransportOrderStatus.CREATED);
            transportOrderEntity.setSchedulingStatus(TransportOrderSchedulingStatus.TO_BE_SCHEDULED);
            // 根据调度配置，查询路线信息
            TransportLineNodeDTO transportLineNodeDTO = transportLineFeign.queryPathByDispatchMethod(sendAgentId, receiveAgentId);
            if (ObjectUtil.isEmpty(transportLineNodeDTO) || CollUtil.isEmpty(transportLineNodeDTO.getNodeList())) {
                throw new SLException(WorkExceptionEnum.TRANSPORT_LINE_NOT_FOUND);
            }
            transportOrderEntity.setNextAgencyId(transportLineNodeDTO.getNodeList().get(1).getId());
            transportOrderEntity.setTransportLine(JSONUtil.toJsonStr(transportLineNodeDTO));
        }
        // 保存运输单
        boolean save = this.save(transportOrderEntity);
        if (!save) {
            throw new SLException(WorkExceptionEnum.TRANSPORT_ORDER_SAVE_ERROR);
        }
        // 保存成功后，发送消息到mq
        // 发送运单创建完成消息
        this.sendTransportOrderCreated(transportOrderEntity);
        // 判断是否需要调度
        if (isDispatch) {
            // 发送运单调度消息到调度中心，参与运单调度
            this.sendTransportOrderMsgToDispatch(transportOrderEntity);
        } else {
            // 不需要调度，更新订单状态
            this.sendUpdateStatusMsg(ListUtil.toList(transportOrderEntity.getId()), TransportOrderStatus.ARRIVED_END);
            // 发送消息生成派件任务
            this.sendDispatchTaskMsgToDispatch(transportOrderEntity);
        }
        return transportOrderEntity;
    }


    @Override
    public Page<TransportOrderEntity> findByPage(TransportOrderQueryDTO transportOrderQueryDTO) {
        Page<TransportOrderEntity> page = new Page<>();
        LambdaQueryWrapper<TransportOrderEntity> queryWrapper = Wrappers.<TransportOrderEntity>lambdaQuery()
                .like(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getId()), TransportOrderEntity::getId, transportOrderQueryDTO.getId())
                .like(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getOrderId()), TransportOrderEntity::getOrderId, transportOrderQueryDTO.getOrderId())
                .eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getStatus()), TransportOrderEntity::getStatus, transportOrderQueryDTO.getStatus())
                .eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getSchedulingStatus()), TransportOrderEntity::getSchedulingStatus, transportOrderQueryDTO.getSchedulingStatus())
                .eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getStartAgencyId()), TransportOrderEntity::getStartAgencyId, transportOrderQueryDTO.getStartAgencyId())
                .eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getEndAgencyId()), TransportOrderEntity::getEndAgencyId, transportOrderQueryDTO.getEndAgencyId())
                .eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getCurrentAgencyId()), TransportOrderEntity::getCurrentAgencyId, transportOrderQueryDTO.getCurrentAgencyId())
                .orderByDesc(TransportOrderEntity::getCreated);
        return super.page(page, queryWrapper);
    }

    @Override
    public TransportOrderEntity findByOrderId(Long orderId) {
        LambdaQueryWrapper<TransportOrderEntity> queryWrapper = Wrappers.<TransportOrderEntity>lambdaQuery()
                .eq(TransportOrderEntity::getOrderId, orderId);
        return super.getOne(queryWrapper);
    }

    @Override
    public List<TransportOrderEntity> findByOrderIds(Long[] orderIds) {
        LambdaQueryWrapper<TransportOrderEntity> queryWrapper = Wrappers.<TransportOrderEntity>lambdaQuery()
                .in(TransportOrderEntity::getOrderId, (Object) orderIds);
        return super.list(queryWrapper);
    }

    @Override
    public List<TransportOrderEntity> findByIds(String[] ids) {
        LambdaQueryWrapper<TransportOrderEntity> queryWrapper = Wrappers.<TransportOrderEntity>lambdaQuery()
                .in(TransportOrderEntity::getId, (Object) ids);
        return super.list(queryWrapper);
    }

    @Override
    public List<TransportOrderEntity> searchById(String id) {
        LambdaQueryWrapper<TransportOrderEntity> queryWrapper = Wrappers.<TransportOrderEntity>lambdaQuery()
                .like(TransportOrderEntity::getId, id);
        return super.list(queryWrapper);
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public boolean updateStatus(List<String> ids, TransportOrderStatus transportOrderStatus) {
        if (TransportOrderStatus.CREATED == transportOrderStatus) {
            // 修改订单状态不能为 新建 状态
            throw new SLException(WorkExceptionEnum.TRANSPORT_ORDER_STATUS_NOT_CREATED);
        }
        List<TransportOrderEntity> transportOrderList;
        // 判断是否为拒收状态，如果是拒收需要重新查询路线，将包裹逆向回去
        if (TransportOrderStatus.REJECTED == transportOrderStatus) {
            // 查询运单列表
            transportOrderList = super.listByIds(ids);
            for (TransportOrderEntity transportOrderEntity : transportOrderList) {
                // 设置为拒收运单
                transportOrderEntity.setIsRejection(true);
                // 根据起始机构规划运输路线，这里要将起点和终点互换
                Long sendAgentId = transportOrderEntity.getEndAgencyId();//起始网点id
                Long receiveAgentId = transportOrderEntity.getStartAgencyId();//终点网点id
                // 起始网点名称是否相等，如果相等则不需要调度，否则需要调度
                boolean isDispatch = !ObjectUtil.equal(sendAgentId, receiveAgentId);
                if (isDispatch) {
                    TransportLineNodeDTO transportLineNodeDTO = this.transportLineFeign.queryPathByDispatchMethod(sendAgentId, receiveAgentId);
                    if (ObjectUtil.hasEmpty(transportLineNodeDTO, transportLineNodeDTO.getNodeList())) {
                        throw new SLException(WorkExceptionEnum.TRANSPORT_LINE_NOT_FOUND);
                    }
                    //删除掉第一个机构，逆向回去的第一个节点就是当前所在节点
                    transportLineNodeDTO.getNodeList().remove(0);
                    transportOrderEntity.setSchedulingStatus(TransportOrderSchedulingStatus.TO_BE_SCHEDULED);//调度状态：待调度
                    transportOrderEntity.setCurrentAgencyId(sendAgentId);//当前所在机构id
                    transportOrderEntity.setNextAgencyId(transportLineNodeDTO.getNodeList().get(0).getId());//下一个机构id
                    //获取到原有节点信息
                    TransportLineNodeDTO transportLineNode = JSONUtil.toBean(transportOrderEntity.getTransportLine(), TransportLineNodeDTO.class);
                    //将逆向节点追加到节点列表中
                    transportLineNode.getNodeList().addAll(transportLineNodeDTO.getNodeList());
                    //合并成本
                    transportLineNode.setCost(NumberUtil.add(transportLineNode.getCost(), transportLineNodeDTO.getCost()));
                    transportOrderEntity.setTransportLine(JSONUtil.toJsonStr(transportLineNode));//完整的运输路线
                }
                //默认参与调度
                // boolean isDispatch = true;
                // if (ObjectUtil.equal(sendAgentId, receiveAgentId)) {
                //     //相同节点，无需调度，直接生成派件任务
                //     isDispatch = false;
                // } else {
                //     TransportLineNodeDTO transportLineNodeDTO = this.transportLineFeign.queryPathByDispatchMethod(sendAgentId, receiveAgentId);
                //     if (ObjectUtil.hasEmpty(transportLineNodeDTO, transportLineNodeDTO.getNodeList())) {
                //         throw new SLException(WorkExceptionEnum.TRANSPORT_LINE_NOT_FOUND);
                //     }
                //     //删除掉第一个机构，逆向回去的第一个节点就是当前所在节点
                //     transportLineNodeDTO.getNodeList().remove(0);
                //     transportOrderEntity.setSchedulingStatus(TransportOrderSchedulingStatus.TO_BE_SCHEDULED);//调度状态：待调度
                //     transportOrderEntity.setCurrentAgencyId(sendAgentId);//当前所在机构id
                //     transportOrderEntity.setNextAgencyId(transportLineNodeDTO.getNodeList().get(0).getId());//下一个机构id
                //     //获取到原有节点信息
                //     TransportLineNodeDTO transportLineNode = JSONUtil.toBean(transportOrderEntity.getTransportLine(), TransportLineNodeDTO.class);
                //     //将逆向节点追加到节点列表中
                //     transportLineNode.getNodeList().addAll(transportLineNodeDTO.getNodeList());
                //     //合并成本
                //     transportLineNode.setCost(NumberUtil.add(transportLineNode.getCost(), transportLineNodeDTO.getCost()));
                //     transportOrderEntity.setTransportLine(JSONUtil.toJsonStr(transportLineNode));//完整的运输路线
                // }
                transportOrderEntity.setStatus(TransportOrderStatus.REJECTED);
                if (isDispatch) {
                    //发送消息参与调度
                    this.sendTransportOrderMsgToDispatch(transportOrderEntity);
                } else {
                    //不需要调度，发送消息生成派件任务
                    transportOrderEntity.setStatus(TransportOrderStatus.ARRIVED_END);
                    this.sendDispatchTaskMsgToDispatch(transportOrderEntity);
                }
            }
        } else {
            // 根据id列表封装成运单对象列表
            transportOrderList = ids.stream().map(id -> {
                // 获取将发往的目的地机构
                Long nextAgencyId = this.getById(id).getNextAgencyId();
                OrganDTO organDTO = organFeign.queryById(nextAgencyId);
                // 构建消息实体类
                String info = CharSequenceUtil.format("快件发往【{}】", organDTO.getName());
                String transportInfoMsg = TransportInfoMsg.builder()
                        .transportOrderId(id)//运单id
                        .status("运送中")//消息状态
                        .info(info)//消息详情
                        .created(DateUtil.current())//创建时间
                        .build().toJson();
                // 发送运单跟踪消息
                this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_INFO, Constants.MQ.RoutingKeys.TRANSPORT_INFO_APPEND, transportInfoMsg);
                // 封装运单对象
                TransportOrderEntity transportOrderEntity = new TransportOrderEntity();
                transportOrderEntity.setId(id);
                transportOrderEntity.setStatus(transportOrderStatus);
                return transportOrderEntity;
            }).collect(Collectors.toList());
        }
        // 批量更新数据
        boolean result = super.updateBatchById(transportOrderList);
        // 发消息通知其他系统运单状态的变化
        this.sendUpdateStatusMsg(ids, transportOrderStatus);
        return result;
    }

    /**
     * 根据运输任务id修改运单状态
     *
     * @param taskId 运输任务id
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public boolean updateByTaskId(Long taskId) {
        // 根据taskId查询运单id集合
        List<String> traansportOrderIdList = transportTaskService.queryTransportOrderIdListById(taskId);
        if (CollUtil.isEmpty(traansportOrderIdList)) {
            return false;
        }
        // 根据运单ids查询运单列表集合
        List<TransportOrderEntity> transportOrderEntityList = listByIds(traansportOrderIdList);
        // 遍历运单列表数据
        for (TransportOrderEntity transportOrder : transportOrderEntityList) {
            // 1.发送物流跟踪消息
            sendTransportOrderInfoMsg(transportOrder, "快件到达【$organId】");
            // 修改运单当前机构的id为下一站点的id，currentAgencyId -> nextAgencyId
            transportOrder.setCurrentAgencyId(transportOrder.getNextAgencyId());
            // 解析完整的运输链路，找到下一个机构id
            Long nextAgencyId = 0L;
            TransportLineNodeDTO transportLineNodeDTO = JSONUtil.toBean(transportOrder.getTransportLine(), TransportLineNodeDTO.class);
            List<OrganDTO> nodeList = transportLineNodeDTO.getNodeList();
            // 这里反向循环主要是考虑到拒收的情况，路线中会存在相同的节点，始终可以查找到后面的节点
            // 正常：A B C D E ，拒收：A B C D E D C B A
            for (int i = nodeList.size() - 1; i >= 0; i--) {
                Long agencyId = nodeList.get(i).getId();
                if (ObjectUtil.equal(agencyId, transportOrder.getCurrentAgencyId())) {
                    if (i == nodeList.size() - 1) {
                        // 已经是最后一个节点了，也就是到最后一个机构了
                        nextAgencyId = agencyId;
                        transportOrder.setStatus(TransportOrderStatus.ARRIVED_END);
                        // 发送消息更新状态
                        this.sendUpdateStatusMsg(ListUtil.toList(transportOrder.getId()), TransportOrderStatus.ARRIVED_END);
                    } else {
                        // 后面还有节点
                        nextAgencyId = nodeList.get(i + 1).getId();
                        // 设置运单状态为待调度
                        transportOrder.setSchedulingStatus(TransportOrderSchedulingStatus.TO_BE_SCHEDULED);
                    }
                    break;
                }
            }
            // 设置下一个节点id
            transportOrder.setNextAgencyId(nextAgencyId);
            // 如果运单没有到达终点，需要发送消息到运单调度的交换机中
            // 如果已经到达最终网点，需要发送消息，进行分配快递员作业
            if (ObjectUtil.notEqual(transportOrder.getStatus(), TransportOrderStatus.ARRIVED_END)) {
                this.sendTransportOrderMsgToDispatch(transportOrder);
            } else {
                // 发送消息生成派件任务
                this.sendDispatchTaskMsgToDispatch(transportOrder);
            }
        }
        return super.updateBatchById(transportOrderEntityList);
    }

    private void sendTransportOrderInfoMsg(TransportOrderEntity transportOrderEntity, String msg) {
        // 获取将发往的目的地机构
        OrganDTO organDTO = organFeign.queryById(transportOrderEntity.getNextAgencyId());
        // 构建消息实体类
        if (CharSequenceUtil.contains(msg, "$organId")) {
            msg = CharSequenceUtil.replace(msg, "$organId", organDTO.getName());
        }
        String info = CharSequenceUtil.format(msg, organDTO.getName());
        String transportInfoMsg = TransportInfoMsg.builder()
                .transportOrderId(transportOrderEntity.getId())//运单id
                .status("运送中")//消息状态
                .info(info)//消息详情
                .created(DateUtil.current())//创建时间
                .build().toJson();
        // 发送运单跟踪消息
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_INFO, Constants.MQ.RoutingKeys.TRANSPORT_INFO_APPEND, transportInfoMsg);
    }

    @Override
    public List<TransportOrderStatusCountDTO> findStatusCount() {
        // 将所有的枚举状态放到集合中
        List<TransportOrderStatusCountDTO> statusCountList = Arrays.stream(TransportOrderStatus.values())
                .map(transportOrderStatus -> TransportOrderStatusCountDTO.builder()
                        .status(transportOrderStatus)
                        .statusCode(transportOrderStatus.getCode())
                        .count(0L)
                        .build())
                .collect(Collectors.toList());
        // 将数量值放入到集合中，如果没有的数量为0
        List<TransportOrderStatusCountDTO> statusList = super.baseMapper.findStatusCount();
        if (CollUtil.isEmpty(statusList)) {
            return statusCountList;
        }
        Map<Integer, TransportOrderStatusCountDTO> statusMap = statusList.stream()
                .collect(Collectors.toMap(TransportOrderStatusCountDTO::getStatusCode, Function.identity(), (key1, key2) -> key2));
        for (TransportOrderStatusCountDTO transportOrderStatusCountDTO : statusCountList) {
            transportOrderStatusCountDTO.setCount(
                    statusMap.getOrDefault(transportOrderStatusCountDTO.getStatusCode(), transportOrderStatusCountDTO).getCount()
            );
        }
        return statusCountList;
    }

    /**
     * 发送消息到调度中心，用于生成取派件任务
     *
     * @param transportOrder 运单
     * @param orderMsg       消息内容
     */
    @Override
    public void sendPickupDispatchTaskMsgToDispatch(TransportOrderEntity transportOrder, OrderMsg orderMsg) {
        // 查询订单对应的位置信息
        OrderLocationDTO orderLocationDTO = this.orderFeign.findOrderLocationByOrderId(orderMsg.getOrderId());
        // (1)运单为空：取件任务取消，取消原因为返回网点；重新调度位置取寄件人位置
        // (2)运单不为空：生成的是派件任务，需要根据拒收状态判断位置是寄件人还是收件人
        // 拒收：寄件人  其他：收件人
        String location;
        if (ObjectUtil.isEmpty(transportOrder)) {
            location = orderLocationDTO.getSendLocation();
        } else {
            location = Boolean.TRUE.equals(transportOrder.getIsRejection()) ? orderLocationDTO.getSendLocation() : orderLocationDTO.getReceiveLocation();
        }
        Double[] coordinate = Convert.convert(Double[].class, CharSequenceUtil.split(location, ","));
        Double longitude = coordinate[0];
        Double latitude = coordinate[1];
        // 设置消息中的位置信息
        orderMsg.setLongitude(longitude);
        orderMsg.setLatitude(latitude);
        // 发送消息,用于生成取派件任务
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.ORDER_DELAYED, Constants.MQ.RoutingKeys.ORDER_CREATE,
                orderMsg.toJson(), Constants.MQ.NORMAL_DELAY);
    }

    @Override
    public PageResponse<TransportOrderDTO> pageQueryByTaskId(Integer page, Integer pageSize, String taskId, String transportOrderId) {
        // 构建分页查询条件
        Page<TransportOrderTaskEntity> transportOrderTaskPage = new Page<>(page, pageSize);
        LambdaQueryWrapper<TransportOrderTaskEntity> queryWrapper = Wrappers.<TransportOrderTaskEntity>lambdaQuery()
                .eq(ObjectUtil.isNotEmpty(taskId), TransportOrderTaskEntity::getTransportTaskId, taskId)
                .like(ObjectUtil.isNotEmpty(transportOrderId), TransportOrderTaskEntity::getTransportOrderId, transportOrderId)
                .orderByDesc(TransportOrderTaskEntity::getCreated);

        // 根据运输任务id、运单id查询运输任务与运单关联关系表
        Page<TransportOrderTaskEntity> pageResult = transportOrderTaskMapper.selectPage(transportOrderTaskPage, queryWrapper);
        if (ObjectUtil.isEmpty(pageResult.getRecords())) {
            return new PageResponse<>(pageResult);
        }
        // 根据运单id查询运单，并转化为dto
        List<String> transportOrderIds = pageResult.getRecords().stream().map(TransportOrderTaskEntity::getTransportOrderId).collect(Collectors.toList());
        List<TransportOrderEntity> entities = baseMapper.selectBatchIds(transportOrderIds);
        // 构建分页结果
        return PageResponse.of(BeanUtil.copyToList(entities, TransportOrderDTO.class), page, pageSize, pageResult.getPages(), pageResult.getTotal());
    }

    private void sendTransportOrderCreated(TransportOrderEntity transportOrderEntity) {
        // 发消息通知其他系统，运单已经生成
        String msg = TransportOrderMsg.builder()
                .id(transportOrderEntity.getId())
                .orderId(transportOrderEntity.getOrderId())
                .created(DateUtil.current())
                .build().toJson();
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_ORDER_DELAYED,
                Constants.MQ.RoutingKeys.TRANSPORT_ORDER_CREATE, msg, Constants.MQ.NORMAL_DELAY);
    }

    /**
     * 发送运单消息到调度中，参与调度
     */
    private void sendTransportOrderMsgToDispatch(TransportOrderEntity transportOrder) {
        Map<Object, Object> msg = MapUtil.builder()
                .put("transportOrderId", transportOrder.getId())
                .put("currentAgencyId", transportOrder.getCurrentAgencyId())
                .put("nextAgencyId", transportOrder.getNextAgencyId())
                .put("totalWeight", transportOrder.getTotalWeight())
                .put("totalVolume", transportOrder.getTotalVolume())
                .put("created", System.currentTimeMillis()).build();
        String jsonMsg = JSONUtil.toJsonStr(msg);
        // 发送消息，延迟5秒，确保本地事务已经提交，可以查询到数据
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_ORDER_DELAYED,
                Constants.MQ.RoutingKeys.JOIN_DISPATCH, jsonMsg, Constants.MQ.LOW_DELAY);
    }

    /**
     * 发送生成取派件任务的消息
     *
     * @param transportOrder 运单对象
     */
    private void sendDispatchTaskMsgToDispatch(TransportOrderEntity transportOrder) {
        //预计完成时间，如果是中午12点到的快递，当天22点前，否则，第二天22点前
        int offset = 0;
        if (LocalDateTime.now().getHour() >= 12) {
            offset = 1;
        }
        LocalDateTime estimatedEndTime = DateUtil.offsetDay(new Date(), offset)
                .setField(DateField.HOUR_OF_DAY, 22)
                .setField(DateField.MINUTE, 0)
                .setField(DateField.SECOND, 0)
                .setField(DateField.MILLISECOND, 0).toLocalDateTime();
        // 发送分配快递员派件任务的消息
        OrderMsg orderMsg = OrderMsg.builder()
                .agencyId(transportOrder.getCurrentAgencyId())
                .orderId(transportOrder.getOrderId())
                .created(DateUtil.current())
                .taskType(PickupDispatchTaskType.DISPATCH.getCode()) //派件任务
                .mark("系统提示：派件前请于收件人电话联系.")
                .estimatedEndTime(estimatedEndTime).build();
        // 发送消息
        this.sendPickupDispatchTaskMsgToDispatch(transportOrder, orderMsg);
    }

    private void sendUpdateStatusMsg(List<String> ids, TransportOrderStatus transportOrderStatus) {
        String msg = TransportOrderStatusMsg.builder()
                .idList(ids)
                .statusName(transportOrderStatus.name())
                .statusCode(transportOrderStatus.getCode())
                .build().toJson();
        //将状态名称写入到路由key中，方便消费方选择性的接收消息
        String routingKey = Constants.MQ.RoutingKeys.TRANSPORT_ORDER_UPDATE_STATUS_PREFIX + transportOrderStatus.name();
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_ORDER_DELAYED, routingKey, msg, Constants.MQ.LOW_DELAY);
    }
}
