package com.qf.bigdata.realtime.dvo;

import java.io.Serializable;

public class SourceChannelDO implements Serializable {

    private String sources;
    private String sourceRemark;
    private String channels;
    private String channelRemark;
    private String media;
    private String mediaRemark;
    private Long ctime;


    public SourceChannelDO(String sources, String sourceRemark, String channels, String channelRemark,String media, String mediaRemark,Long ctime) {
        this.sources = sources;
        this.sourceRemark = sourceRemark;
        this.channels = channels;
        this.channelRemark = channelRemark;
        this.media = media;
        this.mediaRemark = mediaRemark;
        this.ctime = ctime;
    }


    public SourceChannelDO(String sources, String channels, String media) {
        this.sources = sources;
        this.channels = channels;
        this.media = media;
    }

    public String getSources() {
        return sources;
    }

    public void setSources(String sources) {
        this.sources = sources;
    }

    public String getSourceRemark() {
        return sourceRemark;
    }

    public void setSourceRemark(String sourceRemark) {
        this.sourceRemark = sourceRemark;
    }

    public String getChannels() {
        return channels;
    }

    public void setChannels(String channels) {
        this.channels = channels;
    }

    public String getChannelRemark() {
        return channelRemark;
    }

    public void setChannelRemark(String channelRemark) {
        this.channelRemark = channelRemark;
    }

    public String getMedia() {
        return media;
    }

    public void setMedia(String media) {
        this.media = media;
    }

    public String getMediaRemark() {
        return mediaRemark;
    }

    public void setMediaRemark(String mediaRemark) {
        this.mediaRemark = mediaRemark;
    }

    public Long getCtime() {
        return ctime;
    }

    public void setCtime(Long ctime) {
        this.ctime = ctime;
    }
}
