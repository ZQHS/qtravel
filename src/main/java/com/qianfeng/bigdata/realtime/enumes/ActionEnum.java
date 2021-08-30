package com.qianfeng.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;

//行为枚举
public enum ActionEnum {

    INSTALL("01", "install","安装"),
    LAUNCH("02", "launch","加载"),
    LOGIN("03", "login","登录"),
    REGISTER("04", "register","注册"),
    INTERACTIVE("05", "interactive","交互行为"),
    EXIT("06", "exit","退出"),
    PAGE_ENTER_H5("07", "page_enter_h5","页面进入"),
    PAGE_ENTER_NATIVE("08", "page_enter_native","页面进入");
    //PAGE_EXIT("page_exit","页面退出");

    //枚举中的属性
    private String code;
    private String desc;
    private String remark;

    private ActionEnum(String code, String remark, String desc) {
        this.code = code;
        this.remark = remark;
        this.desc = desc;
    }

    //获取action的列表
    public static List<String> getActions(){
        List<String> actions = Arrays.asList(
                INSTALL.code,
                LAUNCH.code,
                //LOGIN.code,
                //REGISTER.code,
                INTERACTIVE.code,
                EXIT.code,
                PAGE_ENTER_H5.code,
                PAGE_ENTER_NATIVE.code
        );
        return actions;
    }

    public static List<String> getMidActions(){
        List<String> actions = Arrays.asList(
                //LAUNCH.code,
                INTERACTIVE.code,
                //PAGE_ENTER_H5.code,
                PAGE_ENTER_NATIVE.code
        );
        return actions;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public String getRemark() {
        return remark;
    }
}