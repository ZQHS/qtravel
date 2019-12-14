package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

public enum NetworkTypeEnum {

    WIFI("0", "无线"),
    D4G("1", "4g"),
    D3G("2", "3g"),
    OFFLINE("3", "线下支付");



    private String code;
    private String desc;

    private NetworkTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getNetworkTypes(){
        List<String> networkTypes = Arrays.asList(
                D4G.code,
                D3G.code,
                WIFI.code,
                OFFLINE.code
        );
        return networkTypes;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
