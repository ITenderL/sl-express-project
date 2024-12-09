package com.sl.ms.carriage.handler;

import com.sl.ms.carriage.domain.constant.CarriageConstant;
import com.sl.ms.carriage.domain.dto.WaybillDTO;
import com.sl.ms.carriage.entity.CarriageEntity;
import com.sl.ms.carriage.handler.AbstractCarriageChainHandler;
import com.sl.ms.carriage.service.CarriageService;
import com.sl.transport.common.util.ObjectUtil;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 同城寄
 */
@Order(100) //定义顺序
@Component
public class SameCityChainHandler extends AbstractCarriageChainHandler {

    @Resource
    private CarriageService carriageService;

    @Override
    public CarriageEntity doHandler(WaybillDTO waybillDTO) {
        CarriageEntity carriageEntity = null;
        if (ObjectUtil.equals(waybillDTO.getReceiverCityId(), waybillDTO.getSenderCityId())) {
            //同城
            carriageEntity = this.carriageService.findByTemplateType(CarriageConstant.SAME_CITY);
        }
        return doNextHandler(waybillDTO, carriageEntity);
    }
}
