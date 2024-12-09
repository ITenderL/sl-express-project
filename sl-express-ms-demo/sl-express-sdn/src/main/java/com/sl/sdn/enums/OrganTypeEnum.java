package com.sl.sdn.enums;

import cn.hutool.core.util.EnumUtil;
import com.sl.transport.common.enums.BaseEnum;
import lombok.Getter;

/**
 * 机构类型枚举
 */
@Getter
public enum OrganTypeEnum implements BaseEnum {

    OLT(1, "一级转运中心"),
    TLT(2, "二级转运中心"),
    AGENCY(3, "网点");

    /**
     * 类型编码
     */
    private final Integer code;

    /**
     * 类型值
     */
    private final String value;

    OrganTypeEnum(Integer code, String value) {
        this.code = code;
        this.value = value;
    }

    public static OrganTypeEnum codeOf(Integer code) {
        return EnumUtil.getBy(OrganTypeEnum::getCode, code);
    }
}
