package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundStreamWordCount {

    public static void main(String[] args) throws Exception {
        //创建流式的执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        //转换文件
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {

            String[] words = line.split(" ");

            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));


        //按照word进行分组
        KeyedStream<Tuple2<String, Long>, String>  wordAndOneKeyedStream= wordAndOneTuple.keyBy(data -> data.f0);

        //分组内进行聚合统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        //打印结果
        sum.print();

        //启动执行
        env.execute();
    }
}
