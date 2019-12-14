package com.qf.bigdata.realtime.util.data.release;

import com.alibaba.fastjson.JSON;
import com.qf.bigdata.realtime.enumes.BiddingStatusEnum;
import com.qf.bigdata.realtime.enumes.BiddingTypeEnum;
import com.qf.bigdata.realtime.enumes.ReleaseStatusEnum;
import com.qf.bigdata.realtime.util.CommonUtil;
import com.qf.bigdata.realtime.util.data.BasicHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReleaseDataHelper extends BasicHelper {


    //反射构造类
    public static final String RLEX_RELEASE_CREATOR = "com.qf.bigdata.releasetime.util.data.release.ReleaseDataHelper";
    public static final int DEVICE_NUM_LENGTH = 8;


    //值key
    public static final String KEY_SID = "sid";
    public static final String KEY_RELEASE_SESSION = "release_session";
    public static final String KEY_RELEASE_STATUS = "release_status";
    public static final String KEY_DEVICE_NUM = "device_num";
    public static final String KEY_DEVICE_TYPE = "device_type";
    public static final String KEY_SOURCES = "sources";
    public static final String KEY_CHANNELS = "channels";
    public static final String KEY_EXTS = "exts";

    public static final String KEY_RELEASE_EXTS_IDCARD = "idcard";//身份证
    public static final String KEY_RELEASE_EXTS_LON = "longitude";//经度
    public static final String KEY_RELEASE_EXTS_LAT = "latitude";//纬度
    public static final String KEY_RELEASE_EXTS_AREA_CODE = "area_code";//地区
    public static final String KEY_RELEASE_EXTS_MATTER_ID = "matter_id";//物料代码
    public static final String KEY_RELEASE_EXTS_MODEL_CODE = "model_code";//模型
    public static final String KEY_RELEASE_EXTS_MODEL_VERSION = "model_version";//模型版本
    public static final String KEY_RELEASE_EXTS_AID = "aid";//广告位id

    //竞价
    public static final String KEY_RELEASE_BIDDING_STATUS = "bidding_status";
    public static final String KEY_RELEASE_BIDDING_TYPE = "bidding_type";
    public static final String KEY_RELEASE_BIDDING_PRICE = "bidding_price";

    public static final String KEY_LAST_PRICE = "last_price";
    public static final String KEY_WIN_PRICE = "win_price";

    //用户注册
    public static final String KEY_RELEASE_USER_REGISTER = "user_register";

    //各个阶段时间
    public static final String KEY_CLICK_CT = "click_ct";
    public static final String KEY_REGISTER_CT = "register_ct";

    //经纬度信息
    //public static final List<String[]>regionInfo = RegionUtil.readRegionFile();
    public static final List<Map<String,String>> AREA_CODES = CommonUtil.getCitys();


    /**
     * 请求id
     */
    public static String sid(Map<String,Object> values){
        //时间参数、时间范围、时间格式化
        String reqRandoms = CommonUtil.getRandomChar(8);
        String sid = reqRandoms;
        try{
            Long ct = (Long)values.getOrDefault(KEY_TIMESTAMP,null);
            sid = ct + reqRandoms;
        }catch(Exception e){
            e.printStackTrace();
        }
        values.put(KEY_SID, sid);
        return sid;
    }

    /**
     * 设备唯一编码
     */
    public static String device_num(Map<String,Object> values){
        String deviceNum = null != values.get(KEY_DEVICE_NUM) ? values.get(KEY_DEVICE_NUM).toString():"";
        return deviceNum;
    }

    /**
     * 投放会话
     * @return
     */
    public static String release_session(Map<String,Object> values){
        String sessionID = null != values.get(KEY_RELEASE_SESSION) ? values.get(KEY_RELEASE_SESSION).toString():"";
        values.put(KEY_RELEASE_SESSION, sessionID);
        return sessionID;
    }


    /**
     * 投放流程环节
     * @return
     */
    public static String release_status(Map<String,Object> values){
        String status = null != values.get(KEY_RELEASE_STATUS) ? values.get(KEY_RELEASE_STATUS).toString():"";
        values.put(KEY_RELEASE_STATUS, status);
        return status;
    }


    /**
     * 设备类型
     * @param values
     * @return
     */
    public static String device_type(Map<String,Object> values){
        String deviceType = null != values.get(KEY_DEVICE_TYPE) ? values.get(KEY_DEVICE_TYPE).toString():"";
        return deviceType;
    }

    /**
     * 渠道
     * @param values
     * @return
     */
    public static String sources(Map<String,Object> values){
        String sources = null != values.get(KEY_SOURCES) ? values.get(KEY_SOURCES).toString():"";
        return sources;
    }


    /**
     * 通道
     * @param values
     * @return
     */
    public static String channels(Map<String,Object> values){
        String channels = null != values.get(KEY_CHANNELS) ? values.get(KEY_CHANNELS).toString():"";
        return channels;
    }


    /**
     * 扩展信息
     * @param values
     * @return
     */
    public static String exts(Map<String,Object> values){
        String exts = "";
        String status = null != values.get(KEY_RELEASE_STATUS) ? values.get(KEY_RELEASE_STATUS).toString():"";
        Map<String,Object> extValues = new HashMap<String,Object>();
        if(ReleaseStatusEnum.RELEASE_NOTCUSTOMER.getCode().equalsIgnoreCase(status) || //非目标客户
                ReleaseStatusEnum.RELEASE_CUSTOMER.getCode().equalsIgnoreCase(status)){//目标客户

            String aid = null != values.get(KEY_RELEASE_EXTS_AID) ? values.get(KEY_RELEASE_EXTS_AID).toString():"";
            List<String> matters = CommonUtil.getRangeNumber(1, 10, 1, "M");
            List<String> models = CommonUtil.getRangeNumber(1, 10, 1, "ML");
            List<String> versions = CommonUtil.getRangeNumber(1, 10, 1, "V");

            //身份证、地区信息
            Map<String,String> areaCodeInfo = CommonUtil.getRandomElementRange(AREA_CODES);
            String areaCode = areaCodeInfo.getOrDefault("adcode","");
            String lon = areaCodeInfo.getOrDefault("longitude","");
            String lat = areaCodeInfo.getOrDefault("latitude","");

            extValues.put(ReleaseDataHelper.KEY_RELEASE_EXTS_IDCARD, CommonUtil.getRandomIDCard());
            extValues.put(ReleaseDataHelper.KEY_RELEASE_EXTS_LON, lon);
            extValues.put(ReleaseDataHelper.KEY_RELEASE_EXTS_LAT, lat);
            extValues.put(ReleaseDataHelper.KEY_RELEASE_EXTS_AREA_CODE, areaCode);

            //物料信息
            extValues.put(ReleaseDataHelper.KEY_RELEASE_EXTS_MATTER_ID, CommonUtil.getRandomElementRange(matters));
            extValues.put(ReleaseDataHelper.KEY_RELEASE_EXTS_MODEL_CODE, CommonUtil.getRandomElementRange(models));
            extValues.put(ReleaseDataHelper.KEY_RELEASE_EXTS_MODEL_VERSION, CommonUtil.getRandomElementRange(versions));
            extValues.put(ReleaseDataHelper.KEY_RELEASE_EXTS_AID, aid);


        }else if(ReleaseStatusEnum.RELEASE_BIDDING.getCode().equalsIgnoreCase(status)){//竞价
            String aid = null != values.get(KEY_RELEASE_EXTS_AID) ? values.get(KEY_RELEASE_EXTS_AID).toString():"";
            Integer price = (Integer)values.getOrDefault(KEY_LAST_PRICE,0);
            String biddingType = null != values.get(KEY_RELEASE_BIDDING_TYPE) ? values.get(KEY_RELEASE_BIDDING_TYPE).toString():"";
            String biddingStatus = null != values.get(KEY_RELEASE_BIDDING_STATUS) ? values.get(KEY_RELEASE_BIDDING_STATUS).toString():"";

            //竞价类型
            if(BiddingTypeEnum.RTB.getCode().equalsIgnoreCase(biddingType)){
                //竞价成功
                if(BiddingStatusEnum.BID_SUC.getCode().equalsIgnoreCase(biddingStatus)){
                    Integer wprice = (Integer)values.getOrDefault(KEY_WIN_PRICE,0);
                    price = wprice;
                }
            }

            extValues.put(ReleaseDataHelper.KEY_RELEASE_BIDDING_STATUS, biddingStatus);
            extValues.put(ReleaseDataHelper.KEY_RELEASE_BIDDING_TYPE, biddingType);
            extValues.put(ReleaseDataHelper.KEY_RELEASE_BIDDING_PRICE, price);
            extValues.put(ReleaseDataHelper.KEY_RELEASE_EXTS_AID, aid);



        }else if(ReleaseStatusEnum.RELEASE_REGISTER.getCode().equalsIgnoreCase(status)){//注册

            extValues.put(ReleaseDataHelper.KEY_RELEASE_USER_REGISTER, CommonUtil.getMobile());
        }else {//曝光 -> 点击 -> 到达
            //没有业务数据，不用处理
        }

        String extjson = JSON.toJSONString(extValues);
        return extjson;
    }


    /**
     * CT
     * @return
     */
    public static Long ct(Map<String,Object> values){
        Long ct = (Long)values.getOrDefault(KEY_TIMESTAMP,null);
        Long clickAndArriveCT = (Long)values.getOrDefault(KEY_CLICK_CT,null);
        Long registerCT = (Long)values.getOrDefault(KEY_REGISTER_CT,null);

        String status = null != values.get(KEY_RELEASE_STATUS) ? values.get(KEY_RELEASE_STATUS).toString():"";
        if(ReleaseStatusEnum.RELEASE_CLICK.getCode().equalsIgnoreCase(status) || ReleaseStatusEnum.RELEASE_ARRIVE.getCode().equalsIgnoreCase(status)){
            ct = clickAndArriveCT;
        }else if(ReleaseStatusEnum.RELEASE_REGISTER.getCode().equalsIgnoreCase(status)){
            ct = registerCT;
        }
        return ct;
    }



}
