package com.sl.transport.enums;

import cn.hutool.core.util.EnumUtil;
import com.sl.transport.common.enums.BaseEnum;
import lombok.Getter;

/**
 * 路线类型枚举
 *
 * @author zzj
 * @version 1.0
 */
@Getter
public enum TransportLineEnum implements BaseEnum {

    TRUNK_LINE(1, "干线", "一级转运中心到一级转运中心", 0.8D),
    BRANCH_LINE(2, "支线", "一级转运中心与二级转运中心之间线路", 1.2D),
    CONNECT_LINE(3, "接驳路线", "二级转运中心到网点", 1.5D),
    SPECIAL_LINE(4, "专线", "任务城市到任意城市", 2.0D),
    TEMP_LINE(5, "临时线路", "任意转运中心到任意转运中心", 2.5D);

    /**
     * 类型编码
     */
    private final Integer code;

    /**
     * 类型值
     */
    private final String value;

    /**
     * 描述
     */
    private final String desc;

    /**
     * 成本
     */
    private final Double cost;

    TransportLineEnum(Integer code, String value, String desc, Double cost) {
        this.code = code;
        this.value = value;
        this.desc = desc;
        this.cost = cost;
    }

    public static TransportLineEnum codeOf(Integer code) {
        return EnumUtil.getBy(TransportLineEnum::getCode, code);
    }

    /**
     * 根据编码获取成本
     */
    public static Double getCostByCode(Integer code) {
        for (TransportLineEnum transportLineEnum : TransportLineEnum.values()) {
            if (transportLineEnum.getCode().equals(code)) {
                return transportLineEnum.getCost();
            }
        }
        return null;
    }
}
