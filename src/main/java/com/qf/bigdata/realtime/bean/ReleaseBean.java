package com.qf.bigdata.realtime.bean;

import java.io.Serializable;

public class ReleaseBean implements Serializable {

    private String sid;
    private String device_num;
    private String device_type;
    private String release_session;
    private String release_status;
    private String sources;
    private String channels;
    private String exts = "";
    private Long ct;

    public ReleaseBean(){

    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getDevice_num() {
        return device_num;
    }

    public void setDevice_num(String device_num) {
        this.device_num = device_num;
    }

    public String getDevice_type() {
        return device_type;
    }

    public void setDevice_type(String device_type) {
        this.device_type = device_type;
    }

    public String getRelease_session() {
        return release_session;
    }

    public void setRelease_session(String release_session) {
        this.release_session = release_session;
    }

    public String getRelease_status() {
        return release_status;
    }

    public void setRelease_status(String release_status) {
        this.release_status = release_status;
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

    public String getExts() {
        return exts;
    }

    public void setExts(String exts) {
        this.exts = exts;
    }

    public Long getCt() {
        return ct;
    }

    public void setCt(Long ct) {
        this.ct = ct;
    }


}
