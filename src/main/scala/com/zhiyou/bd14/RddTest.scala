package com.zhiyou.bd14

import com.zhiyou.bd14.RddTest.sc
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RddTest {

  val conf = new SparkConf(false).setMaster("local[*]").setAppName("My App Map")
//  val sc = SparkContext.getOrCreate(conf)   //单例
  val sc = new SparkContext(conf)
  val file = sc.textFile("/user/user-logs-large.txt",3)





  //flatMap map mapPartitions
  def mapTest(sc:SparkContext):Unit = {

    val mapResult = file.map(x => {
      val info = x.split("\\t")
      (info(0),info(1))
    })
    mapResult.take(10).foreach(println)
  }


  def mapTest01(sc:SparkContext):Unit = {
    val mapPartitionResult = file.mapPartitions(x => {
      var info = new Array[String](3)
      val mapR = for(line <- x) yield {        //yield有返回值
        info = line.split("\\t")
        ( info(0),info(1) )
      }
      mapR.filter(x => x._2=="login")
    })
    mapPartitionResult.take(10).foreach(println)
  }

  //通过转换把一条new_tweet的记录转换成两条login记录
  def flatMapResultTest():Unit = {
    val flatMapResult = file.flatMap(x=>{
      val infos = x.split("\\t")
      infos(1) match {
        case "new_tweet" => for(i <- 1 to 2) yield s"${infos(0)} login ${infos(2)}"
        case _ => Array(x)
      }
    })
    flatMapResult.take(10).foreach(println)
  }


  def distinctTest():Unit = {
    val userRdd = file.map( x => x.split("\\t")(0)).distinct()
    userRdd.foreach(println)
  }

  //filter过滤
  //算子返回true, 该条记录被保留, 返回false, 该条记录被删除
  def filterTest():Unit = {
    val loginFilter = file.filter(x => x.split("\\t")(1) == "login")
    loginFilter.foreach(println)
    println(loginFilter.count())
  }

  def keyByTest():Unit = {
    val userActionType = file.keyBy( x=> {
      val info = x.split("\\t")
      s"${info(0)}---${info(1)}"
    })
    userActionType.take(10).foreach(println)
  }


  def sortByTest() : Unit = {
    val file01 = sc.textFile("file:///D:\\test\\test02\\part-00000")
    val sortBy = file01.sortBy(x => x.split(",")(1).replace(")","").toInt,false,numPartitions = 1)
    sortBy.foreach(println)
  }


  def topN():Unit = {
    val list = List(13,15,21,11,5,19,32)
    val rdd = sc.parallelize(list,2)

    implicit val tonorderd = new Ordering[Int]{
      override def compare(x: Int, y: Int): Int = y.compare(x)
    }

    //从大到小
    val takeOrdered = rdd.takeOrdered(3)
    takeOrdered.foreach(println)

  }

  def topN01():Unit = {
    val list = List(13,15,21,11,5,19,32)
    val rdd = sc.parallelize(list,2)


    //从小到大
    val topN = rdd.top(3)
    topN.foreach(println)
  }

  def topN02():Unit = {
    val list = List(13,15,21,11,5,19,32)
    val rdd = sc.parallelize(list,2)

    //从小到大
    val topN = rdd.top(3)
    topN.foreach(println)
  }



  //重新分区
  def repartitionTest():Unit ={
    val result = file.repartition(5)
    result.foreach(println)
    file.foreachPartition(x => {
      println(s"fileRDD分区, 该分区数据${x.size}条")
    })
    result.foreachPartition(x => {
      var sum = 0
      x.foreach(x => sum+=1)
      println(s"resultRDD分区, 该该分区数据: $sum 条")
    })

    //coalsce分区
    val  coalResult = result.coalesce(3)
    coalResult.foreachPartition(x => {
      println(s"coalResultRDD分区,该分区数据: ${x.size}条")
    })
  }


  def groupBy():Unit ={
    val groupedBy = file.groupBy(x => x.split("\\t")(0))
    groupedBy.foreachPartition(x => {
      println(s"groupedByRDD分区, 该分区共有: ${x.size}条记录")
    })

    groupedBy.foreach(x => {
      println(s"groupedByRDD的一条记录, key为${x._1}, value上集合的记录条数是: ${x._2.size}条")
    })
  }

  //计算用户登录的次数
  def groupedByUserLoginTimes():Unit = {
    val groupedBy = file.groupBy(x => x.split("\\t")(0))
    groupedBy.foreach(x => {
      var sum = 0
      x._2.foreach(line => {
        line.split("\\t")(1) match {
          case "login" => sum += 1
          case _ =>
        }
      })
      println(s"用户: ${x._1}, 登录次数: $sum")
    })
  }


  def aggSumTest():Unit = {
    val list = List(3,5,6,7,4,1,2,8,9)
    val rdd = sc.parallelize(list,3)
    //reduce计算sum
    val reduceResult = rdd.reduce((v1,v2) => v1+v2)
    //fold计算sum
    val foldResult = rdd.fold(0)((v1,v2) => v1+v2)
    //aggregate 把元素两成一个字符串
    val aggregateResult = rdd.aggregate("")((c,v) => {
      c match {
        case "" => v.toString
        case _ => s"$c, $v"
      }
    }, (c1,c2) => {
    c1 match {
      case "" => c2
      case _ => s"$c1, $c2"
    }
  })

    println(s"reduceResult: $reduceResult")
    println(s"foldResult: $foldResult")
    println(s"aggregateResult: $aggregateResult")
  }

  def persistTest() = {
    //计算用户的数量
    //计算ip的数量
    //计算每个用户在每个ip上的登录次数
//    file.cache()
    file.persist(StorageLevel.MEMORY_ONLY)
    val userAmount = file.map(x => x.split("\\t")(0)).distinct().count()

    val ipAmount = file.map(x => x.split("\\t")(2)).distinct().count()

    val userInIpAmount = file.map(x => x.split("\\t")(0)+x.split("\\t")(2)).distinct().count()

    println(userAmount)
    println(ipAmount)
    println(userInIpAmount)
  }












  def main(args: Array[String]): Unit = {
//    mapTest(sc)

    //    mapTest01(sc)

//    flatMapResultTest()

//    distinctTest()

//    filterTest()

//    keyByTest()

//    sortByTest()

//    topN()

//    repartitionTest()

//    groupBy()

//    groupedByUserLoginTimes()

//    aggSumTest()

    persistTest()

    sc.stop()

  }





















}
