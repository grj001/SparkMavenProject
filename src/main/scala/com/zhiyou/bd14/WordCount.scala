package com.zhiyou.bd14

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    //构建SparkConf对象
    //设置分布式运行平台, 和
    // Master 指定运行平台, yarn , standalong, mesos, local
    // local local[N] local[*]
    val conf =
      new SparkConf()
        .setMaster("local[2]")
        .setAppName("wordCount")
    //构建SparkContext对象
    val sc = new SparkContext(conf)
    //加载数据源, 获取Rdd对象
    val fileRdd = sc.textFile("D:\\test\\reversetext\\LICENSE.txt")

    //数据处理开始
    val wordRdd = fileRdd.flatMap(line => line.split("\\s"))
    val result = wordRdd.map(x => (x,1))
        .reduceByKey((v1,v2) => v1+v2)
//    result.saveAsTextFile("D:/test/test01")
    sc.stop()
  }
}
