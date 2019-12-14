package com.qf.bigdata.realtime.domain;

import com.alibaba.fastjson.JSON;
import com.qf.bigdata.realtime.dvo.GisDO;
import com.qf.bigdata.realtime.dvo.SourceChannelDO;
import com.qf.bigdata.realtime.enumes.*;
import com.qf.bigdata.realtime.util.AmapGisUtil;
import com.qf.bigdata.realtime.util.CommonUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;

/**
 * 投放数据
 */
public class ReleaseDO implements Serializable {


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

    private String sid;//请求id
    private String deviceNum; //设备编号
    private String session; //投放会话
    private String status;//状态
    private String deviceType;//设备类型
    private String sources;//渠道
    private String channels;//通道
    private Map<String,Object> exts = new HashMap<String,Object>();//扩展信息
    private Long ct;


    /**
     * 流程状态
     */
    private static List<String> getFlowStatus(String deviceNum) throws Exception {
        List<String> status = new ArrayList<String>();
        if(StringUtils.isNotEmpty(deviceNum)){
            int deviceNumCode = (deviceNum.hashCode() & 0x7FFFFFFF) % ReleaseStatusEnum.getReleaseStatus().size();
            int deviceNumStatus = Integer.valueOf(deviceNumCode);
            int notCustomerStatus = Integer.valueOf(ReleaseStatusEnum.RELEASE_NOTCUSTOMER.getCode());
            int customerStatus = Integer.valueOf(ReleaseStatusEnum.RELEASE_CUSTOMER.getCode());
            if(deviceNumStatus == notCustomerStatus){
                status.add(ReleaseStatusEnum.RELEASE_NOTCUSTOMER.getCode());
            }else{
                for(int i=customerStatus; i<= deviceNumCode; i++){
                    String fStatus = StringUtils.repeat("0",1)+i;
                    status.add(fStatus);
                }
            }

        }
        return status;
    }




    /**
     * 扩展信息
     * @param releaseDO
     * @return
     */
    private static Map<String,Object> exts(ReleaseDO releaseDO, Map<String,Object> values,List<GisDO> giss){
        Map<String,Object> extValues = new HashMap<String,Object>();

        String status = releaseDO.getStatus();
        String aid = null != values.get(KEY_RELEASE_EXTS_AID) ? values.get(KEY_RELEASE_EXTS_AID).toString():"";
        if(ReleaseStatusEnum.RELEASE_NOTCUSTOMER.getCode().equalsIgnoreCase(status) || //非目标客户
                ReleaseStatusEnum.RELEASE_CUSTOMER.getCode().equalsIgnoreCase(status)){//目标客户

            List<String> matters = CommonUtil.getRangeNumber(1, 10, 1, "M");
            List<String> models = CommonUtil.getRangeNumber(1, 10, 1, "ML");
            List<String> versions = CommonUtil.getRangeNumber(1, 10, 1, "V");

            extValues.put(KEY_RELEASE_EXTS_IDCARD, CommonUtil.getRandomIDCard());

            GisDO gisDO = CommonUtil.getRandomElementRange(giss);
            String longitude = gisDO.getLongitude();
            String latitude = gisDO.getLatitude();
            String adcode = gisDO.getAdcode();

            extValues.put(KEY_RELEASE_EXTS_LON, longitude);
            extValues.put(KEY_RELEASE_EXTS_LAT, latitude);
            extValues.put(KEY_RELEASE_EXTS_AREA_CODE, adcode);
            extValues.put(KEY_RELEASE_EXTS_MATTER_ID, CommonUtil.getRandomElementRange(matters));

            extValues.put(KEY_RELEASE_EXTS_MODEL_CODE, CommonUtil.getRandomElementRange(models));
            extValues.put(KEY_RELEASE_EXTS_MODEL_VERSION, CommonUtil.getRandomElementRange(versions));
            extValues.put(KEY_RELEASE_EXTS_AID, aid);


        }else if(ReleaseStatusEnum.RELEASE_BIDDING.getCode().equalsIgnoreCase(status)){//竞价
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

            extValues.put(KEY_RELEASE_BIDDING_STATUS, biddingStatus);
            extValues.put(KEY_RELEASE_BIDDING_TYPE, biddingType);
            extValues.put(KEY_RELEASE_BIDDING_PRICE, price);
            extValues.put(KEY_RELEASE_EXTS_AID, aid);

        }else if(ReleaseStatusEnum.RELEASE_REGISTER.getCode().equalsIgnoreCase(status)){//注册

            extValues.put(KEY_RELEASE_USER_REGISTER, CommonUtil.getMobile());
        }else {//曝光 -> 点击 -> 到达
            //没有业务数据，不用处理
        }

        return extValues;
    }


    private static List<GisDO> getDisDatas(){
        return AmapGisUtil.initDatas();
    }


    /**
     * 模拟投放数据
     * @return
     */
    public static String getReleaseJson(String dts) throws Exception{
        //请求id
        String sid = CommonUtil.getRandomChar(8);


        //设备编号
        String deviceNum =  CommonUtil.getRandomNumStr(8);


        //投放会话
        Long ct = CommonUtil.getRandomTimestamp4CT(dts);
        String sessionRandoms = CommonUtil.getRandomChar(4);
        String session = ct + deviceNum + sessionRandoms;

        //状态
        List<String> releaseStatuss = getFlowStatus(deviceNum);
        String releaseStatus = CommonUtil.getRandomElementRange(releaseStatuss);



        //渠道+通道
        List<SourceChannelDO> sourcesChannels = SourcesChannelEnum.getSourcesChannels();
        SourceChannelDO sc = CommonUtil.getRandomElementRange(sourcesChannels);
        String source = sc.getSources();
        String channel = sc.getChannels();


        ReleaseDO releaseDO = new ReleaseDO();
        releaseDO.setSid(sid);
        releaseDO.setDeviceNum(deviceNum);
        releaseDO.setSession(session);
        releaseDO.setStatus(releaseStatus);
        releaseDO.setSources(source);
        releaseDO.setChannels(channel);

        //扩展
        String aid = CommonUtil.getRandomElementRange(AidEnum.getAids());
        List<String> biddingTypes = BiddingTypeEnum.getBiddingTypes();
        String biddingType = CommonUtil.getRandomElementRange(biddingTypes);
        List<GisDO> gisdos = getDisDatas();
        Map<String,Object> values = new HashMap<String,Object>();
        Integer price = CommonUtil.getRandomNum(2);
        Integer winPrice = CommonUtil.getWinPrice(price);

        long clickAndArriveTime =  CommonUtil.getRandomTimestamp4Release( ct, Calendar.HOUR_OF_DAY, 24);
        long registerTime =  CommonUtil.getRandomTimestamp4Release( ct, Calendar.DATE, 1);

        values.put(KEY_RELEASE_EXTS_AID, aid);
        values.put(KEY_LAST_PRICE, price);
        values.put(KEY_WIN_PRICE, winPrice);
        values.put(KEY_RELEASE_BIDDING_TYPE, biddingType);
        values.put(KEY_CLICK_CT, clickAndArriveTime);
        values.put(KEY_REGISTER_CT, registerTime);

        Map<String,Object> exts = exts(releaseDO, values, gisdos);
        releaseDO.setExts(exts);

        String json = JSON.toJSONString(releaseDO);

        return json;
    }


    /**
     * 模拟投放数据
     * @return
     */
    public static Map<String,String> getReleaseJson4Map(String dts) throws Exception{

        Map<String,String> result = new HashMap<String,String>();


        //请求id
        String sid = CommonUtil.getRandomChar(8);


        //设备编号
        String deviceNum =  CommonUtil.getRandomNumStr(8);


        //投放会话
        Long ct = CommonUtil.getRandomTimestamp4CT(dts);
        String sessionRandoms = CommonUtil.getRandomChar(4);
        String session = ct + deviceNum + sessionRandoms;

        //状态
        List<String> releaseStatuss = getFlowStatus(deviceNum);
        String releaseStatus = CommonUtil.getRandomElementRange(releaseStatuss);



        //渠道+通道
        List<SourceChannelDO> sourcesChannels = SourcesChannelEnum.getSourcesChannels();
        SourceChannelDO sc = CommonUtil.getRandomElementRange(sourcesChannels);
        String source = sc.getSources();
        String channel = sc.getChannels();


        ReleaseDO releaseDO = new ReleaseDO();
        releaseDO.setSid(sid);
        releaseDO.setDeviceNum(deviceNum);
        releaseDO.setSession(session);
        releaseDO.setStatus(releaseStatus);
        releaseDO.setSources(source);
        releaseDO.setChannels(channel);

        //扩展
        String aid = CommonUtil.getRandomElementRange(AidEnum.getAids());
        List<String> biddingTypes = BiddingTypeEnum.getBiddingTypes();
        String biddingType = CommonUtil.getRandomElementRange(biddingTypes);
        List<GisDO> gisdos = getDisDatas();
        Map<String,Object> values = new HashMap<String,Object>();
        Integer price = CommonUtil.getRandomNum(2);
        Integer winPrice = CommonUtil.getWinPrice(price);

        long clickAndArriveTime =  CommonUtil.getRandomTimestamp4Release( ct, Calendar.HOUR_OF_DAY, 24);
        long registerTime =  CommonUtil.getRandomTimestamp4Release( ct, Calendar.DATE, 1);

        values.put(KEY_RELEASE_EXTS_AID, aid);
        values.put(KEY_LAST_PRICE, price);
        values.put(KEY_WIN_PRICE, winPrice);
        values.put(KEY_RELEASE_BIDDING_TYPE, biddingType);
        values.put(KEY_CLICK_CT, clickAndArriveTime);
        values.put(KEY_REGISTER_CT, registerTime);

        Map<String,Object> exts = exts(releaseDO, values, gisdos);
        releaseDO.setExts(exts);


        //hashMD5
        String bysKey = deviceNum  +  sessionRandoms;
        String key = CommonUtil.getMD5AsHex(bysKey.getBytes()).substring(0, 8);
        String json = JSON.toJSONString(releaseDO);
        result.put(key, json);

        return result;
    }


    public static void main(String[] args) throws Exception{

    }


    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getDeviceNum() {
        return deviceNum;
    }

    public void setDeviceNum(String deviceNum) {
        this.deviceNum = deviceNum;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getSources() {
        return sources;
    }

    public void setSources(String sources) {
        this.sources = sources;
    }

    public String getChannels() {
        return channels;
    }

    public void setChannels(String channels) {
        this.channels = channels;
    }

    public Map<String, Object> getExts() {
        return exts;
    }

    public void setExts(Map<String, Object> exts) {
        this.exts = exts;
    }

    public Long getCt() {
        return ct;
    }

    public void setCt(Long ct) {
        this.ct = ct;
    }
}
