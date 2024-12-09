package com.sl.transport.utils;

import cn.hutool.extra.spring.SpringUtil;
import com.sl.transport.common.exception.SLException;
import com.sl.transport.enums.ExceptionEnum;
import com.sl.transport.enums.OrganTypeEnum;
import com.sl.transport.service.AgencyService;
import com.sl.transport.service.IService;
import com.sl.transport.service.OLTService;
import com.sl.transport.service.TLTService;
import lombok.extern.slf4j.Slf4j;

/**
 * 根据type选择对应的service返回
 *
 * @author zzj
 * @version 1.0
 */
@Slf4j
public class OrganServiceFactory {

    public static IService getBean(Integer type) {
        OrganTypeEnum organTypeEnum = OrganTypeEnum.codeOf(type);
        if (null == organTypeEnum) {
            throw new SLException(ExceptionEnum.ORGAN_TYPE_ERROR);
        }
        try {
            switch (organTypeEnum) {
                case AGENCY: {
                    return SpringUtil.getBean(AgencyService.class);
                }
                case OLT: {
                    return SpringUtil.getBean(OLTService.class);
                }
                case TLT: {
                    return SpringUtil.getBean(TLTService.class);
                }
            }
        } catch (Exception e) {
            log.error("获取bean失败", e);
            throw new RuntimeException(e);
        }
        return null;
    }

}
