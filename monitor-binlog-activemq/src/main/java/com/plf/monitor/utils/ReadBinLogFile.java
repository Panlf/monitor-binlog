package com.plf.monitor.utils;

import cn.hutool.core.io.FileUtil;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.*;

/**
 * @author panlf
 * @date 2020/12/25
 */
public class ReadBinLogFile {

    public static void main(String[] args) throws IOException {
        String path = "D:\\Technology\\MySQL\\mysql-8.0.19-winx64\\data\\binlog.000010";
        readBinLogWithStream(path);
    }

    public static void readBinLogWithStream(String path) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        int temp;
        while ((temp = dataInputStream.read()) != -1) {
            // System.out.print(Integer.toBinaryString(temp));
            System.out.println(binaryString(temp));
        }
    }

    /**
     * 将整数转换为对应的8位二进制字符串
     */
    public static String binaryString(int num) {
        StringBuilder result = new StringBuilder();
        int flag = 1 << 7;
        for (int i = 0; i < 8; i++) {
            int val = (flag & num) == 0 ? 0 : 1;
            result.append(val);
            num <<= 1;
        }
        return result.toString();
    }

    public static void readBinLog(String path) throws IOException {
        File binlogFile = new File(path);
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer);
        try {
            for (Event event; (event = reader.readEvent()) != null; ) {
                System.out.println(event.toString());
            }
        } finally {
            reader.close();
        }
    }
}
