package com.sl.ms.carriage.handler;

import com.sl.ms.base.api.common.AreaFeign;
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
 * 省内寄
 */
@Order(200) //定义顺序
@Component
public class SameProvinceChainHandler extends AbstractCarriageChainHandler {

    @Resource
    private CarriageService carriageService;
    @Resource
    private AreaFeign areaFeign;

    @Override
    public CarriageEntity doHandler(WaybillDTO waybillDTO) {
        CarriageEntity carriageEntity = null;
        // 获取收寄件地址省份id
        Long receiverProvinceId = this.areaFeign.get(waybillDTO.getReceiverCityId()).getParentId();
        Long senderProvinceId = this.areaFeign.get(waybillDTO.getSenderCityId()).getParentId();
        if (ObjectUtil.equal(receiverProvinceId, senderProvinceId)) {
            //省内
            carriageEntity = this.carriageService.findByTemplateType(CarriageConstant.SAME_PROVINCE);
        }
        return doNextHandler(waybillDTO, carriageEntity);
    }
}
