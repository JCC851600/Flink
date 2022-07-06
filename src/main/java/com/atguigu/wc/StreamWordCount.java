package com.atguigu.wc;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) {

        //创建流式的执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        DataStreamSource<String> lineDataStreamSource = env.socketTextStream("input/words.txt");

    }
}
