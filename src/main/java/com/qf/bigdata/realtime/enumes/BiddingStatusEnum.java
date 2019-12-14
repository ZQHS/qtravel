package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

public enum BiddingStatusEnum {

    BID_PRICE("01", "竞价出价"),
    BID_SUC("02", "竞价成功");


    private String code;
    private String desc;

    private BiddingStatusEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getBiddingStatus(){
        List<String> biddings = Arrays.asList(
                BID_PRICE.code,
                BID_SUC.code
        );
        return biddings;
    }



    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
