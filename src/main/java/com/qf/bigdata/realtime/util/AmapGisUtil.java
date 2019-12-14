package com.qf.bigdata.realtime.util;

import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import com.qf.bigdata.realtime.constant.CommonConstant;
import com.qf.bigdata.realtime.dvo.GisDO;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.*;

/**
 * 高德api
 */
public class AmapGisUtil implements Serializable {

    private final static Logger log = LoggerFactory.getLogger(AmapGisUtil.class);

    //经度 = 116 + 20 / 60 + 43 / 60 / 60 = 116.34528°
    //纬度 = 39 + 12 / 60 + 37 / 60 / 60 = 39.21028°

    //中国的经纬度范围大约为：纬度3.86~53.55，经度73.66~135.05
    public final static int DEGREE = 60;
    public final static int PFACTOR = 10;
    public final static int FACTOR = 100;
    public final static int CFACTOR = 1000;




    private static final String PARAMS_KEY = "key";
    private static final String PARAMS_OUTPUT = "output";

    /**
     * 高德地图请求秘钥
     */
    private static final String KEY = "83c744016bbc5d85b429516831979886";
    /**
     * 返回值类型
     */
    private static final String OUTPUT_FORMAT = "JSON";
    /**
     * 根据地名获取高德经纬度Api
     */
    private static final String GET_LNG_LAT_URL = "http://restapi.amap.com/v3/geocode/geo";
    /**
     * 根据高德经纬度获取地名Api
     */
    private static final String GET_ADDRESS_URL = "http://restapi.amap.com/v3/geocode/regeo";


    public static final String  RESULT_RESPONSE_JSON = "response_json";
    public static final String RESULT_REQUET_KEY = "requet";
    public static final String RESULT_RESPONSE_KEY = "response";

    public static final String  KEY_LNG = "longitude";
    public static final String  KEY_LAT = "latitude";
    public static final String  KEY_ADCODE = "adcode";
    public static final String  KEY_PROVINCE = "province";
    public static final String  KEY_DISTRICT = "district";
    public static final String  KEY_TOWNCODE = "towncode";
    public static final String  KEY_TOWNSHIP = "township";
    public static final String  KEY_FORMATTED_ADDRESS = "formatted_address";
    public static final String  KEY_ADDRESS = "address";

    //header
    public final static String[] HEADER = new String[]{KEY_LNG, KEY_LAT, KEY_ADCODE,
            KEY_PROVINCE, KEY_DISTRICT, KEY_ADDRESS};


    public static final String GIS_CSV_FILE = "areacode/china_gis_location.csv";

    public static List<GisDO> GL_GISDO = new ArrayList<GisDO>();

    public static List<GisDO> getGISDOs(){
        return initDatas();
    }

    public static List<GisDO> initDatas(){
        try{
            if(CollectionUtils.isEmpty(GL_GISDO)){
                List<Map<String,String>> datas = CSVUtil.readCSVFile(GIS_CSV_FILE,CSVUtil.QUOTE_COMMON);
                for(Map<String,String> data : datas){
                    GisDO gisDO = new GisDO();

                    for(Map.Entry<String,String> entry : data.entrySet()){
                        String key = entry.getKey();
                        String methodKey = StringUtils.capitalize(key);
                        String value = entry.getValue();
                        //ReflexUtil.setFildKeyValues(gisDO, key, value);

                        ReflexUtil.setFildKeyValues4SingleString(gisDO, key, value);
                    }

                    GL_GISDO.add(gisDO);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return GL_GISDO;
    }





    /**
     * 获取经纬度信息
     * @return
     */
    public static Map<String,String> getLngAndLat(String mapUrl,String key, String output, double gdLon, double gdLat){
        Map<String,String> map=new HashMap<String, String>();
        String info = "mapUrl=["+mapUrl+"],key=["+key+"],output=["+output+"],gis=["+gdLon+","+gdLat+"]";
        Map<String,Object> requetData =new HashMap<String, Object>();
        requetData.put(KEY_LNG, String.valueOf(gdLon));
        requetData.put(KEY_LAT, String.valueOf(gdLat));

        String requestValue = JSONObject.toJSONString(requetData);
        String responseValue = "";

        String location = gdLon + "," + gdLat;
        Map<String, String> params = new HashMap<>();
        params.put("location", location);
        try{
            String url = spliceUrl(params, output, key,mapUrl);
            responseValue = accessService (url);

            //log.info("resps={}", resps);
            System.out.println(responseValue);
        }catch (Exception e){
            log.error(e.getMessage());
        }finally {
            map.put(RESULT_REQUET_KEY,requestValue);
            map.put(RESULT_RESPONSE_KEY,responseValue);
        }
        return map;
    }

    /**
     * web服务访问
     * @param url
     * @return
     */
    public static String accessService (String url) {
        StringBuilder json = new StringBuilder();
        try {
            URL oracle = new URL(url);
            URLConnection yc = oracle.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    yc.getInputStream()));
            String inputLine = null;
            while ( (inputLine = in.readLine()) != null) {
                json.append(inputLine);
            }
            in.close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return json.toString();
    }

    /**
     * 拼接请求字符串
     * @param params
     * @param output
     * @param key
     * @param url
     * @return
     * @throws IOException
     */
    private static String spliceUrl(Map<String, String> params, String output, String key, String url) throws IOException {
        StringBuilder baseUrl = new StringBuilder();
        baseUrl.append(url);
        int index = 0;
        Set<Map.Entry<String, String>> entrys = params.entrySet();
        for (Map.Entry<String, String> param : entrys){
            // 判断是否是第一个参数
            if (index == 0) {
                baseUrl.append("?");
            } else {
                baseUrl.append("&");
            }
            baseUrl.append(param.getKey()).append("=").append(URLEncoder.encode(param.getValue(), CommonConstant.CHARSET_UTF8));
            index++;
        }
        baseUrl.append("&"+PARAMS_OUTPUT+"=").append(output).append("&"+PARAMS_KEY+"=").append(key);
        return baseUrl.toString();
    }


    /**
     * 地区
     * @param dict
     * @return
     */
    public static Double getRandomGIS(String dict){
        Double value = null;
        boolean flag = true;
        if(StringUtils.isNotEmpty(dict)){
            int vMin = CommonConstant.LONGITUDE_REGION_MIN.intValue();
            int vMax = CommonConstant.LONGITUDE_REGION_MAX.intValue();
            if(KEY_LAT.equalsIgnoreCase(dict)){
                vMin = CommonConstant.LATITUDE_REGION_MIN.intValue();
                vMax = CommonConstant.LATITUDE_REGION_MAX.intValue();

            }
            int range = vMax - vMin;
            while(flag){
                int degreeInc = new Random().nextInt()%range;
                int fRandomInc = new Random().nextInt(58)+1;
                double fInc = fRandomInc * FACTOR /DEGREE;
                int sRandomInc = new Random().nextInt(58)+1;
                double sInc1 = sRandomInc * FACTOR/DEGREE ;
                double sInc = sInc1 * FACTOR/DEGREE ;

                double fIncd = Double.valueOf(CommonUtil.formatDouble(fInc/FACTOR,"#.00"));
                double sIncd = Double.valueOf(CommonUtil.formatDouble(sInc/FACTOR/FACTOR,"#.00000"));

                value = vMin + degreeInc + fIncd + sIncd;
                if(KEY_LAT.equalsIgnoreCase(dict)){
                    if(value.compareTo(CommonConstant.LATITUDE_REGION_MAX) <= 0){
                        flag=false;
                        break;
                    }
                }else{
                    if(value.compareTo(CommonConstant.LONGITUDE_REGION_MAX) <= 0){
                        flag=false;
                        break;
                    }
                }
            }
        }
        return value;
    }


    /**
     * 北京
     * @param cityCode
     * @param dict
     * @return
     */
    public static Double getQFGIS4BJ(String cityCode,String dict){
        Double value = null;
        boolean flag = true;
        if(StringUtils.isNotEmpty(cityCode) && StringUtils.isNotEmpty(dict)){
            double vMin = CommonConstant.LONGITUDE_QF_MIN;
            double vCompare = CommonConstant.LONGITUDE_QF_MAX;
            if(KEY_LAT.equalsIgnoreCase(dict)){
                vMin = CommonConstant.LATITUDE_QF_MIN;
                vCompare = CommonConstant.LATITUDE_QF_MAX;
            }
            while(flag){
                int fIntInc = new Random().nextInt(10);
                double factor = 100d;
                double cfactor = 100000d;
                int count = 4;
                if(KEY_LAT.equalsIgnoreCase(dict)){
                    factor = 1000d;
                    count = 3;
                }
                int sIntInc = CommonUtil.getRandomNum(count);
                double fInc = new Double(fIntInc) / factor;
                double sInc = new Double(sIntInc) / cfactor;
                double totalInc = fInc + sInc;

                value = vMin + totalInc;
                if(KEY_LAT.equalsIgnoreCase(dict)){
                    if(value.compareTo(vCompare) <= 0){
                        flag=false;
                        break;
                    }
                }else{
                    if(value.compareTo(vCompare) <= 0){
                        flag=false;
                        break;
                    }
                }
            }
        }
        return value;
    }


    /**
     * 制造gis数据
     * @param count
     * @return
     */
    public static List<Object[]> createRandomGISDatas(int count){
        List<Object[]> datas = new ArrayList<Object[]>();
        Gson gson = new Gson();
        if(count <= 10000){
            for(int i=1;i<=count;i++){
                try{
                    double gdLon = getRandomGIS(KEY_LNG);
                    double gdLat = getRandomGIS(KEY_LAT);
                    //"longitude","latitude","adcode","province","district","towncode","township","formatted_address"
                    Map<String,String> resp = AmapGisUtil.getLngAndLat(GET_ADDRESS_URL, KEY, OUTPUT_FORMAT, gdLon, gdLat);
                    log.info("gis={}", JSONObject.toJSON(resp));

                    String reqDatas = resp.getOrDefault(RESULT_REQUET_KEY,"");
                    String respDatas = resp.getOrDefault(RESULT_RESPONSE_KEY,"");

                    String longitude = JsonPath.parse(reqDatas).read("$."+KEY_LNG+"");
                    String latitude = JsonPath.parse(reqDatas).read("$."+KEY_LAT+"");

                    String adcode = JsonPath.parse(respDatas).read("$.regeocode.addressComponent.adcode").toString();
                    String province = JsonPath.parse(respDatas).read("$.regeocode.addressComponent.province").toString();
                    String district = JsonPath.parse(respDatas).read("$.regeocode.addressComponent.district").toString();
                    //String towncode = JsonPath.parse(respDatas).read("$.regeocode.addressComponent.towncode");
                    //String township = JsonPath.parse(respDatas).read("$.regeocode.township");
                    String address = JsonPath.parse(respDatas).read("$.regeocode.formatted_address").toString();

                    Object[] csvDatas = new Object[]{longitude, latitude,
                            adcode, province, district,
                            address
                    };

                    datas.add(csvDatas);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return datas;
    }



    /**
     * 制造gis数据
     * @param count
     * @return
     */
    public static List<Object[]> createRandomGISDatas4BJ(int count){
        List<Object[]> datas = new ArrayList<Object[]>();
        if(count <= 10000){
            for(int i=1;i<=count;i++){

                double gdLon = getQFGIS4BJ("010",KEY_LNG) ;
                double gdLat = getQFGIS4BJ("010",KEY_LAT);
                //"longitude","latitude","adcode","province","district","towncode","township","formatted_address"
                Map<String,String> resp = AmapGisUtil.getLngAndLat(GET_ADDRESS_URL, KEY, OUTPUT_FORMAT, gdLon, gdLat);
                log.info("gis={}", JSONObject.toJSON(resp));

                String reqDatas = resp.getOrDefault(RESULT_REQUET_KEY,"");
                String respDatas = resp.getOrDefault(RESULT_RESPONSE_KEY,"");

                String longitude = JsonPath.parse(reqDatas).read("$."+KEY_LNG+"");
                String latitude = JsonPath.parse(reqDatas).read("$."+KEY_LAT+"");

                String adcode = JsonPath.parse(respDatas).read("$.regeocode.addressComponent.adcode");
                String province = JsonPath.parse(respDatas).read("$.regeocode.addressComponent.province");
                String district = JsonPath.parse(respDatas).read("$.regeocode.addressComponent.district");
                //String towncode = JsonPath.parse(respDatas).read("$.regeocode.addressComponent.towncode");
                //String township = JsonPath.parse(respDatas).read("$.regeocode.township");
                String address = JsonPath.parse(respDatas).read("$.regeocode.formatted_address");

                Object[] csvDatas = new Object[]{longitude, latitude,
                        adcode, province, district,
                        address
                };

                datas.add(csvDatas);
            }
        }
        return datas;
    }

    public static void createRandomGIS(String outpath,List<Object[]> datas){
        try{
            if(StringUtils.isNotEmpty(outpath) && !CollectionUtils.isEmpty(datas)){

                CSVUtil.writeCSVFile(outpath, HEADER, CSVUtil.QUOTE_COMMON, datas, true);
            }
        }catch (Exception e){
            log.error("createRandomGIS.error={}",e.getMessage());
        }
    }


    public static void main(String[] args) throws Exception {

//        String mapUrl = GET_ADDRESS_URL;
//        String key = KEY;
//        String output = OUTPUT_FORMAT;
//
//        for(int i=1;i<2;i++){
//            double gdLon = getQFGIS4BJ("010",LNG_KEY) ;
//            double gdLat = getQFGIS4BJ("010",LAT_KEY);
//
//            Map<String,Object> resp = AmapGisUtil.getLngAndLat(mapUrl, key, output, gdLon, gdLat);
//            log.info("gis={}", JSONObject.toJSON(resp));
//        }

        String outpath = "D:\\qfBigWorkSpace\\release-realtime\\src\\main\\resources\\areacode\\china_gis_location2.csv";
        int count = 10000;
        List<Object[]> gisdatas = createRandomGISDatas(count);

        createRandomGIS(outpath,gisdatas);

        //
//        List<Object[]> gisdatas = createRandomGISDatas4BJ(count);
//        createRandomGIS(outpath, gisdatas);



    }

}
