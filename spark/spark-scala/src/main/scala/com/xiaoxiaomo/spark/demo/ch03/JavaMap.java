package com.xiaoxiaomo.spark.demo.ch03;

import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;

import java.util.HashMap;
import java.util.Map;

/**
 * java 代码
 * javamap 转为scala map
 * Created by xiaoxiaomo on 2016/3/26.
 */
public class JavaMap {


    public static void main(String[] args) {

        // 获取一个java Map
        Map<String, Integer> map = getJavaMap();

        //java map 转换为可变的scala map
        scala.collection.mutable.Map<String, Integer> mutableMap = JavaConverters.mapAsScalaMapConverter(map).asScala();
        System.out.println(mutableMap.keySet());

        //java map 转换为不可变的scala map
        Object obj = Map$.MODULE$.<String,Integer>newBuilder().$plus$plus$eq(mutableMap.toSeq());
        Object result = ((scala.collection.mutable.Builder) obj).result();

        scala.collection.immutable.Map<String,String> immutableMap = (scala.collection.immutable.Map)result;
        System.out.println(immutableMap.keySet());
    }


    public static Map<String,Integer> getJavaMap(){
        Map<String,Integer> map = new HashMap<String,Integer>() ;
        map.put("a",1);
        map.put("b", 2);
        return map ;
    }

}
