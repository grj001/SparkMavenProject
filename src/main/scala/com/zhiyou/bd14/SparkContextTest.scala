package com.zhiyou.bd14

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object SparkContextTest {

  val conf = new SparkConf().setMaster("local[*]").setAppName("My app")
  val sc = new SparkContext(conf)

  def test() :Unit = {
    //读取sequenceFile
    val seqRdd = sc.sequenceFile("/firstseqfile",classOf[IntWritable], classOf[Text])
    seqRdd.foreach(println)
  }

  def test01() :Unit ={
    //使用paralize构建rdd
    val list = List(1,2,3,4,5,6,7)
    val listToRdd = sc.parallelize(list)
    println(listToRdd)
    val sum = listToRdd.reduce(_+_)
    println("**************************"+sum)
  }

  def main(args: Array[String]): Unit = {

    test()

    test01()




    sc.stop()
  }

}
