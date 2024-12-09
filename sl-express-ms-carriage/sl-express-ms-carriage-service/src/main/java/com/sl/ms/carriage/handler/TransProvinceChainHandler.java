package com.sl.ms.carriage.handler;

import com.sl.ms.carriage.domain.constant.CarriageConstant;
import com.sl.ms.carriage.domain.dto.WaybillDTO;
import com.sl.ms.carriage.entity.CarriageEntity;
import com.sl.ms.carriage.handler.AbstractCarriageChainHandler;
import com.sl.ms.carriage.service.CarriageService;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 跨省
 */
@Order(400) //定义顺序
@Component
public class TransProvinceChainHandler extends AbstractCarriageChainHandler {

    @Resource
    private CarriageService carriageService;

    @Override
    public CarriageEntity doHandler(WaybillDTO waybillDTO) {
        CarriageEntity carriageEntity = this.carriageService.findByTemplateType(CarriageConstant.TRANS_PROVINCE);
        return doNextHandler(waybillDTO, carriageEntity);
    }
}
