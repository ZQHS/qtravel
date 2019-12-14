package com.qf.bigdata.realtime.bean;

import java.io.Serializable;

public class SourcesBean implements Serializable {

    private String sources;
    private String sources_remark;
    private String channels;
    private String channels_remark;
    private String media_type;
    private String media_type_remark;
    private Long ctime;

    public SourcesBean(){

    }

    public String getSources() {
        return sources;
    }

    public void setSources(String sources) {
        this.sources = sources;
    }

    public String getSources_remark() {
        return sources_remark;
    }

    public void setSources_remark(String sources_remark) {
        this.sources_remark = sources_remark;
    }

    public String getChannels() {
        return channels;
    }

    public void setChannels(String channels) {
        this.channels = channels;
    }

    public String getChannels_remark() {
        return channels_remark;
    }

    public void setChannels_remark(String channels_remark) {
        this.channels_remark = channels_remark;
    }

    public String getMedia_type() {
        return media_type;
    }

    public void setMedia_type(String media_type) {
        this.media_type = media_type;
    }

    public String getMedia_type_remark() {
        return media_type_remark;
    }

    public void setMedia_type_remark(String media_type_remark) {
        this.media_type_remark = media_type_remark;
    }

    public Long getCtime() {
        return ctime;
    }

    public void setCtime(Long ctime) {
        this.ctime = ctime;
    }
}
