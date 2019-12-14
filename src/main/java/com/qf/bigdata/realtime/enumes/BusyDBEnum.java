package com.qf.bigdata.realtime.enumes;

import com.qf.bigdata.realtime.constant.TravelConstant;
import org.apache.commons.lang3.StringUtils;

/**
 * 业务数据
 */
public enum BusyDBEnum {

    REALEASE_CUSTOMER("release","customer", "01", "目前客户"),
    REALEASE_NOTCUSTOMER("release","notcustomer", "00", "非目标客户"),
    REALEASE_BIDDING("release","bidding", "02", "竞价环节"),
    REALEASE_SHOW("release","show", "03", "曝光环节"),
    REALEASE_CLICK("release","click", "04", "点击环节"),
    REALEASE_ARRIVE("release","arrive","05", "到达环节"),
    REALEASE_REGISTER("release","register", "06", "注册环节");

    BusyDBEnum(String db, String table, String code, String desc) {
        this.db = db;
        this.table = table;
        this.desc = desc;
    }


    private String db;

    private String table;

    private String desc;

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public String getDesc() {
        return desc;
    }

    public String getDBTable() {
        return db+ TravelConstant.BOTTOM_LINE+desc;
    }

    public static BusyDBEnum getByCode(String code, String table) {
        if (StringUtils.isBlank(code) || StringUtils.isBlank(table)) {
            return null;
        }
        for (BusyDBEnum typeEnum : values()) {
            if (typeEnum.getDb().equals(code) && typeEnum.getTable().equals(table)) {
                return typeEnum;
            }
        }
        return null;
    }
}
