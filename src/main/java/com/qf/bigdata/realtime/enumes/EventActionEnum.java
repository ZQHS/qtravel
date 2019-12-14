package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

public enum EventActionEnum {

    PRODUCT_KEEP("101", "产品收藏"),
    PRODUCT_CS("102", "产品客服"),
    PRODUCT_SHARE("103", "产品分享"),
    PRODUCT_COMMENT("104", "产品点评"),
    PRODUCT_PREFERENTIAL("105", "产品领劵");


    private String code;
    private String desc;

    private EventActionEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getEventActions(){
        List<String> targetActions = Arrays.asList(
                PRODUCT_KEEP.code,
                PRODUCT_CS.code,
                PRODUCT_SHARE.code,
                PRODUCT_COMMENT.code,
                PRODUCT_PREFERENTIAL.code
                );
        return targetActions;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
