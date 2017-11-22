package com.zhiyou.bd14

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

object PairRddTest {

  val conf = new SparkConf().setMaster("local[*]").setAppName("pairrdd test")
  val sc = SparkContext.getOrCreate(conf)

  def mapValueTest() = {
    val map = List("小张" -> 3000, "小王" -> 3500, "小李" -> 8000)
    val rdd = sc.parallelize(map)
    //map 每个人的薪水涨2000
    val mapResult = rdd.mapValues(x => x + 2000)
    mapResult.foreach(println)
  }


  def flatMapValuesTest() = {
    val map = List("小张" -> 3000, "小王" -> 3500, "小李" -> 8000)
    val rdd = sc.parallelize(map)

    //flatMapValues 只对value进行展平, 而key
    // 根据每个人的薪水打标签, 比方说大于无钱使高收入, 大于7000 是土豪, 大于5000 是低收入
    val flatMapResult = rdd.flatMapValues(x => {
      val tag1 = x match {
        case n if n < 5000 => "低收入"
        case _ => "高收入"
      }
      x match {
        case n if n > 7000 => Array(tag1, "土豪")
        case _ => Array(tag1)
      }
    })
    flatMapResult.foreach(println)

    val keysResult = rdd.keys
    val valuesResult = rdd.values

    keysResult.foreach(println)
    valuesResult.foreach(println)
  }


  def lookUpTest() = {
    val map = List("小张" -> ("语文", 88), "小王" -> ("英语", 92), "小张" -> ("数学", 20))
    val rdd = sc.parallelize(map)
    // key是小张 的所有记录
    val seq = rdd.lookup("小张")
    seq.foreach(println)
  }

  def collectAsMapTest() = {
    val map = List("小张" -> ("语文", 88), "小王" -> ("英语", 92), "小张" -> ("数学", 20))
    val rdd = sc.parallelize(map)

    //collectmap
    val cmap = rdd.collectAsMap()
    cmap.foreach(println)
  }

  def countByKeyTest() = {
    val map = List("小张" -> ("语文", 88), "小王" -> ("英语", 92), "小张" -> ("数学", 20))
    val rdd = sc.parallelize(map)

    //countByKey
    val countByKey = rdd.countByKey()
    countByKey.foreach(println)
  }


  def aggragateTest() = {
    //reduceByKey
    val scores = List("小张 语文 20", "小张 数学 80", "小张 语文 40", "小李 语文 70", "小李 英语 40", "小王 语文 10")
    val rdd = sc.parallelize(scores)
    //计算每个学生的总分数
    val reduceRdd = rdd.map(x => {
      val regex = "(.+)\\s(.+)\\s(.+)".r
      x match {
        case regex(studentName, className, score) => (studentName, score.toInt)
      }
    })
    val reduceResult = reduceRdd.reduceByKey((v1, v2) => v1 + v2)
    reduceResult.foreach(println)
  }

  def aggragateTest01() = {
    //reduceByKey
    val scores = List("小张 语文 20", "小张 数学 80", "小张 语文 40", "小李 语文 70", "小李 英语 40", "小王 语文 10")
    val rdd = sc.parallelize(scores)
    //计算每个学生的总分数
    val reduceRdd = rdd.map(x => {
      val regex = "(.+)\\s(.+)\\s(.+)".r
      x match {
        case regex(studentName, className, score) => (studentName, score.toInt)
      }
    })

    //foldByKey
    val foldResult = reduceRdd.foldByKey(0)((v1, v2) => v1 + v2)
    foldResult.foreach(println)
  }

  def aggragateTest02() = {
    //reduceByKey
    val scores = List("小张 语文 20", "小张 数学 80", "小张 语文 40", "小李 语文 70", "小李 英语 40", "小王 语文 10")
    val rdd = sc.parallelize(scores)
    //计算每个学生的总分数
    val reduceRdd = rdd.map(x => {
      val regex = "(.+)\\s(.+)\\s(.+)".r
      x match {
        case regex(studentName, className, score) => (studentName, score.toInt)
      }
    })

    //foldByKey
    val aggregateResult = reduceRdd.aggregateByKey(0)(
      seqOp = (c, v) => c + v
      , combOp = (c1, c2) => c1 + c2
    )
  }

  def groupByKey() = {
    //reduceByKey
    val scores = List("小张 语文 20", "小张 数学 80", "小张 语文 40", "小李 语文 70", "小李 英语 40", "小王 语文 10")
    val rdd = sc.parallelize(scores)
    //计算每个学生的总分数
    val reduceRdd = rdd.map(x => {
      val regex = "(.+)\\s(.+)\\s(.+)".r
      x match {
        case regex(studentName, className, score) => (studentName, score.toInt)
      }
    })
    val groupByKey = reduceRdd.groupByKey()
    groupByKey.foreach(x => {
      println(s"${x._1} 总分数: ${x._2.sum}")
    })
  }

  def groupByKey01() = {
    //reduceByKey
    val scores = List("小张 语文 20", "小张 数学 80", "小张 语文 40", "小李 语文 70", "小李 英语 40", "小王 语文 10")
    val rdd = sc.parallelize(scores)
    //计算每个学生的总分数
    val reduceRdd = rdd.map(x => {
      val regex = "(.+)\\s(.+)\\s(.+)".r
      x match {
        case regex(studentName, className, score) => (studentName, score.toInt)
      }
    })
    //combineByKey
    val combineResult = reduceRdd.combineByKey[Int](
      (initValue: Int) => initValue
      , (c: Int, v: Int) => c + v
      , (c1: Int, c2: Int) => c1 + c2
    )
  }

  //列出每个人的每一学科的成绩如下: 小张: 语文 40 数学: 30. 英语: 10
  def combineByKeyTest() = {

    val scores = List("小张 语文 20"
      , "小张 数学 80"
      , "小张 语文 40"
      , "小李 语文 70"
      , "小李 英语 40"
      , "小王 语文 10"
      , "小王 语文 20")
    val rdd = sc.parallelize(scores)

    val combineRDD = rdd.map(
      x => {
        val info = x.split("\\s")
        (info(0), s"${info(1)} ${info(2)}")
      }
    )
    val result = combineRDD.combineByKey(
      (fv: String) => fv.split("\\s").mkString(":")    //对value进行处理
      , (c: String, v: String) => s"${c}, ${v.split("\\s").mkString(":")}"    //对下一个value值的处理
      , (c1: String, c2: String) => s"$c1, $c2"
    )

    result.foreach(x => println(s"${x._1} ${x._2}"))
  }




  def cogroupTest() = {
    val list1 = List("小张"->"语文 50", "小张"->"数学 60", "小王"->"语文 70"
    , "小王"-> "英语 50", "小李"->"数学 80")
    val list2 = List("小张"->"迟到 10", "小张"->"旷课 20","小王"->"迟到 2"
    , "小王"->"旷课 0")
    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)

    val cogroupRDD = rdd1.cogroup(rdd2)
    cogroupRDD.foreach(x => {
      println(s"姓名:${x._1}, 成绩:${x._2._1}, 违纪信息:${x._2._2}")
    })
    println(s"结果数据记录: ${cogroupRDD.count}")
  }


  //剪集
  def subtractTest() ={
    val list1 = List("小张"->"语文 50", "小张"->"数学 60", "小王"->"语文 70"
      , "小王"-> "英语 50", "小李"->"数学 80")
    val list2 = List("小张"->111,"小刘"->666)
    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)
    val subResult = rdd1.subtractByKey(rdd2)
    subResult.foreach(println)
  }


  def joinTest() = {
    //两个rdd要进行join操作
    val list1 = List("小张"->"男","小李"->"女","小王"->"男","小刘"->"女")
    val list2 = List("小张"->"23岁","小王"->"18岁","小刘"->"19岁","小赵"->"22岁")
    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)

    //内关联
    val innerJoinResult = rdd1.join(rdd2)
    innerJoinResult.foreach(x => {
      println(s"姓名:${x._1}, 性别: ${x._2._1}, 年龄: ${x._2._2}")
    })

//    //左外关联
//    val leftJoinResult = rdd1.leftOuterJoin(rdd2)
//    leftJoinResult.foreach(x => {
//      val age = x._2._2 match {
//        case None=>"不详"
//        case Some(a) => a
//      }
//      println(s"姓名: ${x._1}, 性别: ${x._2._1}, 年龄: ${age}")
//    })

    //右外关联
//    val rightJoinResult = rdd1.rightOuterJoin(rdd2)
//    rightJoinResult.foreach(x => {
//      val gender = x._2._1 match {
//        case None => "不详"
//        case Some(a) => a
//      }
//      println(s"姓名: ${x._1}, 性别:${gender}, 年龄: ${x._2._2}")
//    })

    //全外连接
    val fullOutterJoinResult = rdd1.fullOuterJoin(rdd2)
    fullOutterJoinResult.foreach(x => {
      val gender = x._2._1 match {
        case None => "-"
        case Some(a) => a
      }
      val age =  x._2._2 match {
        case None => "-"
        case Some(a) => a
      }
      println(s"姓名:${x._1}, 性别:${gender}, 年龄:${age}")
    })

  }


  //saveAsNewAPIHadoopDataSet  把rdd的数据保存到hbase的方法
  //saveAsNewAPIHadoopFile     把rdd的数据按照指定
  // kv saveAs 文件的方式 保存数据
  def saveFile() = {
    val list = List("banana","apple","pear","orange","watermalon")
    val rdd = sc.parallelize(list).map(x => (x, x.length))
    //保存成sequence文件
//    rdd.saveAsSequenceFile("/user/spark-sequence-file")

//    import org.apache.hadoop.mapred.TextOutputFormat
//    rdd.saveAsHadoopFile("/user/spark-sequence-file1"
//      ,classOf[Text]
//      ,classOf[IntWritable]
//      ,classOf[TextOutputFormat[Text,IntWritable]])


    //import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
    rdd.saveAsNewAPIHadoopFile("/spark-sequence-file2"
    , classOf[Text]
    , classOf[IntWritable]
    , classOf[TextOutputFormat[Text, IntWritable]])

  }

  //共享变量



  def main(args: Array[String]): Unit = {
    //    mapValueTest()
    //    flatMapValuesTest()
    //    lookUpTest()
    //    collectAsMapTest()
    //    countByKeyTest()
    //    aggragateTest()
    //    aggragateTest02()
    //    groupByKey()
//    combineByKeyTest()
//    cogroupTest()
//    subtractTest()
//    joinTest()
//    joinTest()
    saveFile()








  }


}
