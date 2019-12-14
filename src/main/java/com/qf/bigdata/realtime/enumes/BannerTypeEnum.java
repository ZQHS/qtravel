package com.qf.bigdata.realtime.enumes;

import org.apache.commons.lang3.StringUtils;

/**
 * 广告位类型
 */
public enum BannerTypeEnum {
    QQ_BANNER_1001("qq", "10-1"),
    QQ_BANNER_0802("qq", "8-2");

    BannerTypeEnum(String code, String desc) {
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

    public static BannerTypeEnum getByCode(String code) {
        if (StringUtils.isBlank(code)) {
            return null;
        }
        for (BannerTypeEnum typeEnum : values()) {
            if (typeEnum.getCode().equals(code)) {
                return typeEnum;
            }
        }
        return null;
    }
}
