package com.qf.bigdata.realtime.enumes;

public enum SourceEnum {

    TOPLINE("topline", "头条"),
    NETYEX("netyex", "网易YEX"),
    XIMALAYA("ximalaya", "喜马拉雅"),
    LIEBAO("liebao", "猎豹"),
    TENCENT("tencent", "腾讯"),
    BAIDU("baidu", "百度"),
    DOUYIN("douyin", "抖音");


    private String code;
    private String desc;

    private SourceEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
