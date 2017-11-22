package com.zhiyou.bd14

import org.apache.spark.{SparkConf, SparkContext}

object ShareVariables {

  val conf = new SparkConf().setMaster("local[*]").setAppName("共享变量")
  val sc = SparkContext.getOrCreate(conf)


  //在driver上定义, 在executor上可以使用的变量, 叫共享变量
  def varTest() = {
    //统计每个用户的访问次数, 然后过滤掉小于times的数据, 并返回结果
    val times = 50
    val rdd = sc.textFile("/user/user-logs-large.txt")

    val result = rdd.map(x => (x.split("\\t")(0), 1))
      .reduceByKey(_ + _)
      .filter(x => x._2 > times)

    result.foreach(println)
  }


  def accumulateTest() = {
    //统计每个用户的访问此时, 同时计算出大于50 和小于50 的用户数有多少
    val rdd = sc.textFile("/user/user-logs-large.txt")

    val result = rdd.map(x => (x.split("\\t")(0), 1))
      .reduceByKey(_ + _)

    var sumBt50 = 0
    var sumLt50 = 0

    //使用累加器, 来完成这种统计
    val sumBtAccmulator = sc.longAccumulator("bt50")
    val sumLtAccmulator = sc.longAccumulator("lt50")

    result.foreach(x => {
      if (x._2 > 50) sumBt50 += 1 else sumLt50 += 1
      if (x._2 > 50) sumBtAccmulator.add(1) else sumLtAccmulator.add(1)

      println(x)
    })
    println(s"大于50的用户数: $sumBt50")
    println(s"小于50的用户数: $sumLt50")
    println(s"小于50的用户数: ${sumBtAccmulator.value}")
    println(s"小于50的用户数: ${sumLtAccmulator.value}")
  }

  def accumulateTest01() = {

    //文件中有多少记录
    //统计每个用户的访问此时, 同时计算出大于50 和小于50 的用户数有多少
    val rdd = sc.textFile("/user/user-logs-large.txt")
    val recordNumberAccmulator = sc.longAccumulator("recordNumber")

    val result = rdd.map(x => {
      recordNumberAccmulator.add(1)
      (x.split("\\t")(0), 1)
    }
    ).reduceByKey(_ + _)

    println(s"user-logs-large.txt的总计路数为: ${recordNumberAccmulator.value}")


  }

  def accumulateTest02() = {

    //List(1,2,43,5,6,7,76,8,89,0,3)--转换成rdd, 提取出该rdd中所有的偶数
    //同时计算出, rdd中的总记录数, 奇数的记录数, 占比
    val list = List(1, 2, 43, 5, 6, 7, 76, 8, 89, 0, 3)
    val rdd = sc.parallelize(list)


    //记录数
    val allNum = sc.longAccumulator("allNumber")
    val oddNum = sc.longAccumulator("oddNumber")
    val evenNum = sc.longAccumulator("evenNumber")

    val result = rdd.map(x => {
      allNum.add(1)
      x%2 match {
        case 0 => evenNum.add(1)
        case 1 => oddNum.add(1)
        case _ =>
      }
      (x,1)
    })

    val resultEvenNum = result.filter(x => x._1%2==0)



    resultEvenNum.foreach(println)

    println(s"总记录数为:${allNum.value}" +
      s", 偶数的记录数为: ${evenNum.value}, 占比:${}" +
      s", 奇数的记录数为${oddNum.value}, 占比${}")

  }


  //广播变量
  def boradcastTest(filterNo: Int) = {
    //计算每个用户访问次数, 过滤掉访问次数小于filterNo的记录
    val rdd = sc.textFile("/user/user-logs-large.txt")
      .map(x => (x.split("\\t")(0), 1))
      .reduceByKey(_ + _)

    //把filterNo声明为广播变量, 而不直接使用filterNo
    val broadcastFilterNo = sc.broadcast(filterNo)
    val result = rdd.filter(x => x._2 > broadcastFilterNo.value)

    result.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    //    varTest()
    //    accumulateTest()
//    accumulateTest01()
//    boradcastTest(500)
    accumulateTest02()
  }


}
