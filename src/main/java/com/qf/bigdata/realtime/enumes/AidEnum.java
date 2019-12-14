package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

public enum AidEnum {

    AID_101("1001", "10-1位"),
    AID_102("1002", "10-2位"),
    AID_103("1003", "10-3位"),
    AID_81("0801", "8-1位"),
    AID_82("0802", "8-2位");


    private String code;
    private String desc;

    private AidEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    public static List<String> getAids(){
        List<String> aids = Arrays.asList(
                AID_101.code,
                AID_102.code,
                AID_103.code,
                AID_81.code,
                AID_82.code
        );
        return aids;
    }


    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
