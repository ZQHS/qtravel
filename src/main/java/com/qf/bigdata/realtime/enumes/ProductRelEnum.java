package com.qf.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

public enum ProductRelEnum {

    CLASS("2", "分类"),
    SHOPER("3", "店铺"),
    GOODS("4", "商品"),
    BRAND("5", "品牌");


    private String code;
    private String desc;

    private ProductRelEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    public static List<String> getProductRels(){
        List<String> actions = Arrays.asList(
                CLASS.code,
                SHOPER.code,
                GOODS.code,
                BRAND.code
        );
        return actions;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
