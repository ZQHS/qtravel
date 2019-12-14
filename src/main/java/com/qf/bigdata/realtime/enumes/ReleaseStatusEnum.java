package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

/**
 * Created by finup on 2018/5/14.
 */
public enum ReleaseStatusEnum {

    RELEASE_NOTCUSTOMER("00", "非目标客户"),
    RELEASE_CUSTOMER("01", "目标客户"),
    RELEASE_BIDDING("02", "竞价"),
    RELEASE_SHOW("03", "曝光"),
    RELEASE_CLICK("04", "点击"),
    RELEASE_ARRIVE("05", "到达"),
    RELEASE_REGISTER("06", "注册");


    private String code;
    private String desc;

    private ReleaseStatusEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    public static List<String> getReleaseStatus(){
        List<String> status = Arrays.asList(
                RELEASE_NOTCUSTOMER.code,
                RELEASE_CUSTOMER.code,
                RELEASE_BIDDING.code,
                RELEASE_SHOW.code,
                RELEASE_CLICK.code,
                RELEASE_ARRIVE.code,
                RELEASE_REGISTER.code
        );
        return status;
    }



    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

}
