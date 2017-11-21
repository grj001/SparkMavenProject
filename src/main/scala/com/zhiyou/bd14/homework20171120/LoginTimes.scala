package com.zhiyou.bd14.homework20171120

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, Test}


object LoginTimes {

  //计算登陆次数
  def main(args:Array[String]):Unit = {
    //构建SparkConf对象
    //设置分布运行平台, 和Master
    val conf = new SparkConf().setMaster("local").setAppName("CalculateLoginTimes")
    //构建SparkContext对象
    val sc = new SparkContext(conf)

    val fileRdd = sc.textFile("file:///C:\\Users\\Administrator\\Desktop\\SVN\\user-logs-large.txt")
    //打印出全部的数据
//    fileRdd.foreach(println)

    //数据处理
    /*
    jim	logout	93.24.237.12
    mike	new_tweet	87.124.79.252
    bob	new_tweet	58.133.120.100

    这种数据进行解析

     */
    // 打印出每一行的数据
    // 如果这一行的行为为login, 则把它添加入计算的列表
    val loginActionTimes =
    fileRdd
      .flatMap(line => line.split("\\n"))
      .filter(x => x.split("\\s")(1).equals("login"))
    //这个是一行 bob	new_tweet	58.133.120.100


    val result = loginActionTimes.map(x => (x.split("\\s")(0),1) ).reduceByKey((v1, v2) => v1+v2)
    result.saveAsTextFile("file:///D:\\test\\test02")


    sc.stop()
  }








}

