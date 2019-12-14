package com.qf.bigdata.realtime.util.json;

/**
 * Created by hp on 2017/5/1.
 */
public class JsonFormat {

    String jsonPath;
    String dataName;
    String datatype;
    String dataFormat;
    String dataAliasName;

    public JsonFormat(String jsonPath, String dataName,
                      String datatype, String dataFormat, String dataAliasName){
        this.jsonPath = jsonPath;
        this.dataName = dataName;
        this.datatype = datatype;
        this.dataFormat = dataFormat;
        this.dataAliasName = dataAliasName;
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public void setJsonPath(String jsonPath) {
        this.jsonPath = jsonPath;
    }

    public String getDataName() {
        return dataName;
    }

    public void setDataName(String dataName) {
        this.dataName = dataName;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }

    public String getDataAliasName() {
        return dataAliasName;
    }

    public void setDataAliasName(String dataAliasName) {
        this.dataAliasName = dataAliasName;
    }
}
