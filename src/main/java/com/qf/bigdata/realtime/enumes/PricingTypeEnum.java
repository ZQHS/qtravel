package com.qf.bigdata.realtime.enumes;

import org.apache.commons.lang3.StringUtils;

/**
 * 计费方式
 */
public enum PricingTypeEnum {
    CPM("CMP", "千次展示付费");

    PricingTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    private String code;

    private String lowerCode;

    private String desc;

    public String getCode() {
        return code;
    }


    public String getDesc() {
        return desc;
    }

    public static PricingTypeEnum getByCode(String code) {
        if (StringUtils.isBlank(code)) {
            return null;
        }
        for (PricingTypeEnum typeEnum : values()) {
            if (typeEnum.getCode().equals(code)) {
                return typeEnum;
            }
        }
        return null;
    }
}
