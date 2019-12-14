package com.qf.bigdata.realtime.util;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GisUtil implements Serializable {

    private final static Logger log = LoggerFactory.getLogger(GisUtil.class);

    public static final String BAIDU_MAP_API_URL = "http://api.map.baidu.com/geocoder/v2/?";

    public static final String BAIDU_MAP_API_KEY = "C5dae37ba0a97216555b8bf2b76bacfd";

    public static final String BAIDU_MAP_API_OUTPUT = "json";

    public static final String  LNG_KEY = "lng";
    public static final String  LAT_KEY = "lat";

    /**
     * 获取经纬度信息
     * @param address
     * @return
     */
    public static Map<String,String> getLngAndLat(String mapUrl,String address,String output,String key){
        Map<String,String> map=new HashMap<String, String>();
        String url = mapUrl+"address="+address+"&output="+output+"&ak="+key;
        System.out.println("url："+url);
        String json = loadJSON(url);
        JSONObject obj = JSONObject.parseObject(json);
        if(obj.get("status").toString().equals("0")){
            String lng=obj.getJSONObject("result").getJSONObject("location").getDouble("lng").toString();
            String lat=obj.getJSONObject("result").getJSONObject("location").getDouble("lat").toString();
            map.put(LNG_KEY, lng);
            map.put(LAT_KEY, lat);
            System.out.println("经度："+lng+"---纬度："+lat);
        }else{
            log.info("%s not found");
            //System.out.println(address + "未找到相匹配的经纬度！");
        }
        return map;
    }


    public static String loadJSON (String url) {
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
     * gis信息
     * @return
     * @throws Exception
     */
    public static List<Object[]> getGis4Baidu(List<Map<String,String>> areaCodes) throws Exception{
        List<Object[]> areaGiss = new ArrayList<Object[]>();

        //转gis信息
        if(null != areaCodes){
            for(Map<String,String> areaCode : areaCodes){
                String name = areaCode.getOrDefault("name","");
                String level = areaCode.getOrDefault("level","");
                String code = areaCode.getOrDefault("code","");

                //定位
                String mapUrl = BAIDU_MAP_API_URL;
                String address = name;
                String output = BAIDU_MAP_API_OUTPUT;
                String key = BAIDU_MAP_API_KEY;
                Map<String,String> gisMap = getLngAndLat( mapUrl, address, output, key);
                String lng = gisMap.getOrDefault(LNG_KEY,"");
                String lat = gisMap.getOrDefault(LAT_KEY,"");
                Object[] gisInfo = new Object[]{name, code, level, lng, lat};
                areaGiss.add(gisInfo);
                Thread.sleep(100 * 2);
            }
        }

        return areaGiss;
    }




    public static void main(String[] args) throws Exception {



    }

}
