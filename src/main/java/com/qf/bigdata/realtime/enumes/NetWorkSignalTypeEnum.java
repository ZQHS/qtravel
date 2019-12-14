package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

public enum NetWorkSignalTypeEnum {

    POWER("9", "强"),
    ORDINARY("1", "一般"),
    WEAK("0", "弱");


    private String code;
    private String desc;

    private NetWorkSignalTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    public static List<String> getNetWorkSignalTypes(){
        List<String> vehicleTypes = Arrays.asList(
                POWER.code,
                ORDINARY.code,
                WEAK.code
        );
        return vehicleTypes;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

}
