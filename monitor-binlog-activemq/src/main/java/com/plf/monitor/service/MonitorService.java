package com.plf.monitor.service;

import cn.hutool.core.collection.CollUtil;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;


@Component
@Slf4j
public class MonitorService {

    @Value("${monitor.mysql.host}")
    private String host;

    @Value("${monitor.mysql.port}")
    private Integer port;

    @Value("${monitor.mysql.username}")
    private String username;

    @Value("${monitor.mysql.password}")
    private String password;

    @PostConstruct
    public void startBinlogListener() {
        log.info("日志监听程序开启...");

        BinaryLogClient client = new BinaryLogClient(host, port, username, password);
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                // 时间类型成为Long类型，这样就不能判断获取到的Long类型是否是Date的类型
                //EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener((event) -> {
            EventData eventData = event.getData();
            if(eventData != null){
                if(eventData instanceof UpdateRowsEventData){
                    UpdateRowsEventData updateRowsEventData = (UpdateRowsEventData) eventData;
                    dealUpdateData(updateRowsEventData);
                }else if(eventData instanceof WriteRowsEventData){
                    WriteRowsEventData writeRowsEventData = (WriteRowsEventData) eventData;
                    dealWriteData(writeRowsEventData);
                }else if(eventData instanceof DeleteRowsEventData){
                    DeleteRowsEventData deleteRowsEventData = (DeleteRowsEventData) eventData;
                    dealDeleteData(deleteRowsEventData);
                }else if(eventData instanceof RowsQueryEventData){
                    RowsQueryEventData rowsQueryEventData = (RowsQueryEventData) eventData;
                    dealRowsQueryData(rowsQueryEventData);
                }
            }
        });
        try {
            log.info("监听程序已经正常启动...");
            client.connect();
        } catch (IOException e) {
            log.error("监听程序出现错误,错误原因:{}", e.getMessage());
        }
    }

    private void analysisData(Serializable[] serializes) {
        if(serializes!=null && serializes.length>0){
            for(int i=0;i<serializes.length;i++){
                Serializable serializable = serializes[i];
                if(serializable instanceof byte[]){
                    log.info("sql语句中的varchar数据值为:{}",new String((byte[]) serializable));
                }else{
                    log.info("sql语句中数据类型:{},数据值为:{}",serializable.getClass().getName(),serializable);
                }
            }
        }
    }

    public void dealRowsQueryData(RowsQueryEventData rowsQueryEventData){
        if(rowsQueryEventData!=null){
            String query = rowsQueryEventData.getQuery();
            log.info("获取当前的Query语句:{}",query);
        }
    }

    public void dealWriteData(WriteRowsEventData writeRowsEventData){
        log.info("dealWriteData start deal data");
        if(writeRowsEventData!=null){
            List<Serializable[]> list =  writeRowsEventData.getRows();
            if(CollUtil.isNotEmpty(list)){
                for(Serializable[] serializes:list){
                    analysisData(serializes);
                }
            }
        }
    }

    public void dealDeleteData(DeleteRowsEventData deleteRowsEventData){
        log.info("dealDeleteData start deal data");
        log.info("delete data info:{}",deleteRowsEventData.toString());
        if(deleteRowsEventData!=null){
            List<Serializable[]> list =  deleteRowsEventData.getRows();
            if(CollUtil.isNotEmpty(list)){
                for(Serializable[] serializes:list){
                    analysisData(serializes);
                }
            }
        }
    }
    public void dealUpdateData(UpdateRowsEventData updateRowsEventData){
        log.info("dealUpdateData start deal data");
        log.info("update data info:{}",updateRowsEventData.toString());
        if(updateRowsEventData!=null) {
            List<Map.Entry<Serializable[], Serializable[]>> list = updateRowsEventData.getRows();
            if (CollUtil.isNotEmpty(list)) {
                for (Map.Entry<Serializable[], Serializable[]> map : list) {
                    //Before的数据
                    //map.getKey();
                    //After的数据
                    Serializable[] serializes = map.getValue();
                    analysisData(serializes);
                }
            }
        }
    }
}
