package com.sl.ms.carriage.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.stream.StreamUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.sl.ms.carriage.domain.constant.CarriageConstant;
import com.sl.ms.carriage.domain.dto.CarriageDTO;
import com.sl.ms.carriage.domain.dto.WaybillDTO;
import com.sl.ms.carriage.entity.CarriageEntity;
import com.sl.ms.carriage.enums.CarriageExceptionEnum;
import com.sl.ms.carriage.handler.CarriageChainHandler;
import com.sl.ms.carriage.mapper.CarriageMapper;
import com.sl.ms.carriage.service.CarriageService;
import com.sl.ms.carriage.utils.CarriageUtils;
import com.sl.transport.common.exception.SLException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author analytics
 * @date 2024/10/19 23:11
 * @description
 */
@Slf4j
@Service
public class CarriageServiceImpl extends ServiceImpl<CarriageMapper, CarriageEntity> implements CarriageService {

    @Resource
    private CarriageChainHandler carriageChainHandler;

    @Transactional(rollbackFor = Exception.class)
    @Override
    public CarriageDTO saveOrUpdate(CarriageDTO carriageDto) {
        log.info("新增运费模板 --> {}", carriageDto);
        // 思路：首先根据条件查询运费模板，判断模板是否存在，如果不存在直接新增
        // 如果存在，需要判断是否为经济区互寄，如果不是，抛出异常，如果是，需要进一步判断所关联的城市是否重复
        // 如果重复，抛出异常，如果不重复进行新增或更新
        LambdaQueryWrapper<CarriageEntity> queryWrapper = Wrappers.<CarriageEntity>lambdaQuery()
                .eq(CarriageEntity::getTemplateType, carriageDto.getTemplateType())
                .eq(CarriageEntity::getTransportType, CarriageConstant.REGULAR_FAST);
        // 查询到模板列表
        List<CarriageEntity> carriageList = super.list(queryWrapper);
        if (ObjectUtil.notEqual(carriageDto.getTemplateType(), CarriageConstant.ECONOMIC_ZONE)) {
            // 非经济区互寄的情况下，需要判断查询的模板是否为空
            // 如果不为空并且入参的参数id为空，说明是新增操作，非经济区只能有一个模板，需要抛出异常
            if (ObjectUtil.isNotEmpty(carriageList) && ObjectUtil.isEmpty(carriageDto.getId())) {
                // 新增操作，模板重复，抛出异常
                throw new SLException(CarriageExceptionEnum.NOT_ECONOMIC_ZONE_REPEAT);
            }
            //新增或更新非经济区模板
            return this.saveOrUpdateCarriage(carriageDto);
        }
        //判断模板所关联的城市是否有重复
        //查询其他模板中所有的经济区列表
        List<String> associatedCityList = StreamUtil.of(carriageList)
                //排除掉自己，检查与其他模板是否存在冲突
                .filter(carriageEntity -> ObjectUtil.notEqual(carriageEntity.getId(), carriageDto.getId()))
                //获取关联城市
                .map(CarriageEntity::getAssociatedCity)
                //将关联城市按照逗号分割
                .map(associatedCity -> CharSequenceUtil.split(associatedCity, ','))
                //将上面得到的集合展开，得到字符串
                .flatMap(StreamUtil::of)
                //收集到集合中
                .collect(Collectors.toList());
        //查看当前新增经济区是否存在重复，取交集来判断是否重复
        Collection<String> intersection = CollUtil.intersection(associatedCityList, carriageDto.getAssociatedCityList());
        if (CollUtil.isNotEmpty(intersection)) {
            //有重复
            throw new SLException(CarriageExceptionEnum.ECONOMIC_ZONE_CITY_REPEAT);
        }
        //新增或更新经济区模板
        return this.saveOrUpdateCarriage(carriageDto);
    }

    public CarriageDTO saveOrUpdateCarriage(CarriageDTO carriageDto) {
        CarriageEntity entity = CarriageUtils.toEntity(carriageDto);
        super.saveOrUpdate(entity);
        return CarriageUtils.toDTO(entity);
    }

    @Override
    public List<CarriageDTO> findAll() {
        LambdaQueryWrapper<CarriageEntity> queryWrapper = Wrappers.<CarriageEntity>lambdaQuery().orderByDesc(CarriageEntity::getCreated);
        List<CarriageEntity> list = super.list(queryWrapper);
        return list.stream().map(CarriageUtils::toDTO).collect(Collectors.toList());
    }

    @Override
    public CarriageDTO compute(WaybillDTO waybillDTO) {
        //根据参数查找运费模板
        CarriageEntity carriage = this.carriageChainHandler.findCarriage(waybillDTO);
        //计算重量，确保最小重量为1kg
        double computeWeight = this.getComputeWeight(waybillDTO, carriage);
        //计算运费，首重 + 续重
        double expense = carriage.getFirstWeight() + ((computeWeight - 1) * carriage.getContinuousWeight());
        //保留一位小数
        expense = NumberUtil.round(expense, 1).doubleValue();
        //封装运费和计算重量到DTO，并返回
        CarriageDTO carriageDTO = CarriageUtils.toDTO(carriage);
        carriageDTO.setExpense(expense);
        carriageDTO.setComputeWeight(computeWeight);
        return carriageDTO;
    }

    @Override
    public CarriageEntity findByTemplateType(Integer templateType) {
        if (ObjectUtil.equals(templateType, CarriageConstant.ECONOMIC_ZONE)) {
            throw new SLException(CarriageExceptionEnum.METHOD_CALL_ERROR);
        }
        LambdaQueryWrapper<CarriageEntity> queryWrapper = Wrappers.lambdaQuery(CarriageEntity.class)
                .eq(CarriageEntity::getTemplateType, templateType)
                .eq(CarriageEntity::getTransportType, CarriageConstant.REGULAR_FAST);
        return super.getOne(queryWrapper);
    }


    /**
     * 根据体积参数与实际重量计算计费重量
     *
     * @param waybillDTO 运费计算对象
     * @param carriage   运费模板
     * @return 计费重量
     */
    private double getComputeWeight(WaybillDTO waybillDTO, CarriageEntity carriage) {
        //计算体积，如果传入体积不需要计算
        Integer volume = waybillDTO.getVolume();
        if (ObjectUtil.isEmpty(volume)) {
            try {
                //长*宽*高计算体积
                volume = waybillDTO.getMeasureLong() * waybillDTO.getMeasureWidth() * waybillDTO.getMeasureHigh();
            } catch (Exception e) {
                //计算出错设置体积为0
                volume = 0;
            }
        }
        // 计算体积重量，体积 / 轻抛系数
        BigDecimal volumeWeight = NumberUtil.div(volume, carriage.getLightThrowingCoefficient(), 1);
        //取大值
        double computeWeight = NumberUtil.max(volumeWeight.doubleValue(), NumberUtil.round(waybillDTO.getWeight(), 1).doubleValue());
        //计算续重，规则：不满1kg，按1kg计费；10kg以下续重以0.1kg计量保留1位小数；
        // 10-100kg续重以0.5kg计量保留1位小数；100kg以上四舍五入取整
        if (computeWeight <= 1) {
            return 1;
        }
        if (computeWeight <= 10) {
            return computeWeight;
        }
        // 举例：
        // 108.4kg按照108kg收费
        // 108.5kg按照109kg收费
        // 108.6kg按照109kg收费
        if (computeWeight >= 100) {
            return NumberUtil.round(computeWeight, 0).doubleValue();
        }
        //0.5为一个计算单位，举例：
        // 18.8kg按照19收费，
        // 18.4kg按照18.5kg收费
        // 18.1kg按照18.5kg收费
        // 18.6kg按照19收费
        int integer = NumberUtil.round(computeWeight, 0, RoundingMode.DOWN).intValue();
        if (NumberUtil.sub(computeWeight, integer) == 0) {
            return integer;
        }
        if (NumberUtil.sub(computeWeight, integer) <= 0.5) {
            return NumberUtil.add(integer, 0.5);
        }
        return NumberUtil.add(integer, 1);
    }
}
