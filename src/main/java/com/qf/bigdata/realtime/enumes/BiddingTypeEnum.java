package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

public enum BiddingTypeEnum {

    PMP("PMP", "定价"),
    RTB("RTB", "竞价");


    private String code;
    private String desc;

    private BiddingTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getBiddingTypes(){
        List<String> biddingTypes = Arrays.asList(
                PMP.code,
                RTB.code
        );
        return biddingTypes;
    }


    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
