package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

public enum EventActionEnum {

    PRODUCT_KEEP("101", "收藏"),
    PRODUCT_APPLAUD("102", "点赞"),
    PRODUCT_SHARE("103", "分享"),
    PRODUCT_COMMENT("104", "点评"),
    PRODUCT_CS("105", "客服");


    private String code;
    private String desc;

    private EventActionEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getEventActions(){
        List<String> targetActions = Arrays.asList(
                PRODUCT_KEEP.code,
                PRODUCT_SHARE.code,
                PRODUCT_COMMENT.code,
                PRODUCT_APPLAUD.code
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
