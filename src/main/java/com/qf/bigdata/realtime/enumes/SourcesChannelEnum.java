package com.qf.bigdata.realtime.enumes;


import com.qf.bigdata.realtime.dvo.SourceChannelDO;

import java.util.Arrays;
import java.util.List;

public enum SourcesChannelEnum {

    TOPLINE("topline", "头条","c1","通道1","4","信息流"),
    NETYEX("netyex", "网易YEX","c1","通道1","4","信息流"),
    XIMALAYA("ximalaya", "喜马拉雅","c1","通道1","4","信息流"),
    LIEBAO("liebao", "猎豹","c1","通道1","4","信息流"),
    TENCENT("tencent", "腾讯","c1","通道1","4","信息流"),
    BAIDU("baidu", "百度","c1","通道1","4","信息流"),
    DOUYIN("douyin", "抖音","c1","通道1","4","信息流");


    private String sources;
    private String sourceRemark;
    private String channels;
    private String channelRemark;
    private String media;
    private String mediaRemark;

    private SourcesChannelEnum(String sources, String sourceRemark, String channels, String channelRemark,String media, String mediaRemark) {
        this.sources = sources;
        this.sourceRemark = sourceRemark;
        this.channels = channels;
        this.channelRemark = channelRemark;
        this.media = media;
        this.mediaRemark = mediaRemark;
    }


    public static List<SourceChannelDO> getSourcesChannels(){
        List<SourceChannelDO> sourceChannels = Arrays.asList(
                new SourceChannelDO(TOPLINE.sources, TOPLINE.channels, TOPLINE.media),
                new SourceChannelDO(NETYEX.sources, NETYEX.channels, NETYEX.media),
                new SourceChannelDO(XIMALAYA.sources, XIMALAYA.channels, XIMALAYA.media),
                new SourceChannelDO(LIEBAO.sources, LIEBAO.channels, LIEBAO.media),
                new SourceChannelDO(TENCENT.sources, TENCENT.channels, TENCENT.media),
                new SourceChannelDO(BAIDU.sources, BAIDU.channels, BAIDU.media),
                new SourceChannelDO(DOUYIN.sources, DOUYIN.channels, DOUYIN.media)
        );
        return sourceChannels;
    }

    public String getSourceRemark() {
        return sourceRemark;
    }

    public String getChannels() {
        return channels;
    }

    public String getChannelRemark() {
        return channelRemark;
    }

    public String getMedia() {
        return media;
    }

    public String getMediaRemark() {
        return mediaRemark;
    }
}
