package com.sl.ms.work.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.sl.ms.work.domain.enums.transportorder.TransportOrderSchedulingStatus;
import com.sl.ms.work.entity.TransportOrderEntity;
import com.sl.ms.work.entity.TransportOrderTaskEntity;
import com.sl.ms.work.mapper.TransportOrderTaskMapper;
import com.sl.ms.work.service.TransportOrderTaskService;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 运单表 服务实现类
 */
@Service
public class TransportOrderTaskServiceImpl extends
        ServiceImpl<TransportOrderTaskMapper, TransportOrderTaskEntity> implements TransportOrderTaskService {

    @Override
    public void createTransportOrderTask(Long transportTaskId, JSONObject jsonObject) {
        // // 创建运输任务与运单之间的关系
        // JSONArray transportOrderIdList = jsonObject.getJSONArray("transportOrderIdList");
        // if (CollUtil.isEmpty(transportOrderIdList)) {
        //     return;
        // }
        // // 将运单id列表转成运单实体列表
        // List<TransportOrderTaskEntity> resultList = transportOrderIdList.stream()
        //         .map(o -> {
        //             TransportOrderTaskEntity transportOrderTaskEntity = new TransportOrderTaskEntity();
        //             transportOrderTaskEntity.setTransportTaskId(transportTaskId);
        //             transportOrderTaskEntity.setTransportOrderId(Convert.toStr(o));
        //             return transportOrderTaskEntity;
        //         }).collect(Collectors.toList());
        // // 批量保存运输任务与运单的关联表
        // saveBatch(resultList);
        // // 批量标记运单为已调度状态
        // List<TransportOrderEntity> list = transportOrderIdList.stream()
        //         .map(o -> {
        //             TransportOrderEntity transportOrderEntity = new TransportOrderEntity();
        //             transportOrderEntity.setId(Convert.toStr(o));
        //             //状态设置为已调度
        //             transportOrderEntity.setSchedulingStatus(TransportOrderSchedulingStatus.SCHEDULED);
        //             return transportOrderEntity;
        //         }).collect(Collectors.toList());
        // transportOrderService.updateBatchById(list);
    }

    @Override
    public void batchSaveTransportOrder(List<TransportOrderTaskEntity> transportOrderTaskList) {
        saveBatch(transportOrderTaskList);
    }

    @Override
    public IPage<TransportOrderTaskEntity> findByPage(Integer page, Integer pageSize, String transportOrderId, Long transportTaskId) {
        Page<TransportOrderTaskEntity> iPage = new Page(page, pageSize);
        LambdaQueryWrapper<TransportOrderTaskEntity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.like(ObjectUtil.isNotEmpty(transportOrderId), TransportOrderTaskEntity::getTransportOrderId, transportOrderId);
        lambdaQueryWrapper.like(ObjectUtil.isNotEmpty(transportTaskId), TransportOrderTaskEntity::getTransportTaskId, transportTaskId);
        return super.page(iPage, lambdaQueryWrapper);
    }

    @Override
    public List<TransportOrderTaskEntity> findAll(String transportOrderId, Long transportTaskId) {
        LambdaQueryWrapper<TransportOrderTaskEntity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.like(ObjectUtil.isNotEmpty(transportOrderId), TransportOrderTaskEntity::getTransportOrderId, transportOrderId);
        lambdaQueryWrapper.like(ObjectUtil.isNotEmpty(transportTaskId), TransportOrderTaskEntity::getTransportTaskId, transportTaskId);
        lambdaQueryWrapper.orderBy(true, false, TransportOrderTaskEntity::getCreated);
        return list(lambdaQueryWrapper);
    }

    @Override
    public Long count(String transportOrderId, Long transportTaskId) {
        LambdaQueryWrapper<TransportOrderTaskEntity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.like(ObjectUtil.isNotEmpty(transportOrderId), TransportOrderTaskEntity::getTransportOrderId, transportOrderId);
        lambdaQueryWrapper.like(ObjectUtil.isNotEmpty(transportTaskId), TransportOrderTaskEntity::getTransportTaskId, transportTaskId);
        return super.count(lambdaQueryWrapper);
    }

    @Override
    public void del(String transportOrderId, Long transportTaskId) {
        if (ObjectUtil.isAllEmpty(transportOrderId, transportTaskId)) {
            return;
        }
        LambdaQueryWrapper<TransportOrderTaskEntity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.eq(ObjectUtil.isNotEmpty(transportOrderId), TransportOrderTaskEntity::getTransportOrderId, transportOrderId);
        lambdaQueryWrapper.eq(ObjectUtil.isNotEmpty(transportTaskId), TransportOrderTaskEntity::getTransportTaskId, transportTaskId);
        super.remove(lambdaQueryWrapper);
    }
}
