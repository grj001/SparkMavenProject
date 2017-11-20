package com.zhiyou.bd14

import org.apache.spark.{SparkConf, SparkContext}

object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    //构建SparkConf对象
    //设置分布式运行平台, 和
    // Master 指定运行平台, yarn , standalong, mesos, local
    // local local[N] local[*]
    val conf =
    new SparkConf()
      .setMaster("spark://master:7077")
      .setAppName("wordCount")
    //构建SparkContext对象
    val sc = new SparkContext(conf)
    sc.addJar("D:\\develop\\ideaWorkspace\\SparkMavenProject\\out\\artifacts\\SparkMavenProject_jar\\SparkMavenProject.jar")
    //加载数据源, 获取Rdd对象
    //file:///c:\\desktop\\forwc
    // hdfs://master:9000/path/to/file
    val fileRdd = sc.textFile("/user/user-logs-large.txt")
    fileRdd.foreach(println)

    //数据处理开始
    val wordRdd = fileRdd.flatMap(line => line.split("\\s"))
    val result = wordRdd.map(x => (x, 1))
      .reduceByKey((v1, v2) => v1 + v2)
//    result.saveAsTextFile("/user/Spark")

    result.foreach(println)

//    val rdd2 = sc.textFile("")
//    fileRdd.flatMap(_.split("\\s"))
//      .map((_,1))
//      .reduceByKey(_+_)
//        .saveAsTextFile("/bd14/sparkwc")
    sc.stop()
  }
}
