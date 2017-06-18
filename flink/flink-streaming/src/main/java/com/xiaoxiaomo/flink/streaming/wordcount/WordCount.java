package com.xiaoxiaomo.flink.streaming.wordcount;

import com.xiaoxiaomo.flink.streaming.wordcount.util.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * 一个简单的单词统计
 *
 * run:
 *
 * ./bin/flink run -c com.xiaoxiaomo.flink.streaming.wordcount.WordCount flink-streaming-1.0.0-jar-with-dependencies.jar [--input localfile] [--output localfile]
 */
public class WordCount {


    public static void main(String[] args) throws Exception {

        //1. 获取参数和环境
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // get default test text data
            text = env.fromElements(WordCountData.WORDS);
        }


        //2. 处理数据
        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0).sum(1);

        //3. 结果
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        // execute program
        env.execute("Streaming WordCount");
    }



    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
