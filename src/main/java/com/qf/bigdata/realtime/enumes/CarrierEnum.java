package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

public enum CarrierEnum {

    CHINA_MOBILE("1", "中国移动"),
    CHINA_UNICOM("2", "中国联通"),
    CHINA_TElECOM("3", "中国电信");


    private String code;
    private String desc;

    private CarrierEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getCarriers(){
        List<String> carriers = Arrays.asList(
                CHINA_MOBILE.code,
                CHINA_UNICOM.code,
                CHINA_TElECOM.code
        );
        return carriers;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
