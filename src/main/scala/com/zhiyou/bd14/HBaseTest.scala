package com.zhiyou.bd14

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object HBaseTest {

  val conf = new SparkConf().setMaster("local[*]")
    .setAppName("spark读写 HBase")
  val sc = SparkContext.getOrCreate(conf)


  //统计每个用户在ip上的行为次数, 好存到HBase中, user_ip_times, rowkey 用户名
  // ,列簇i
  // 列成员, ip, 列: times


  def writeToHBase() = {
    val configuration = HBaseConfiguration.create(sc.hadoopConfiguration)
    //设置保存到hbase里面的配置信息
    configuration.set(TableOutputFormat.OUTPUT_TABLE, "sparktest:user_ip_times")
    var job = Job.getInstance(configuration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    FileOutputFormat.setOutputPath(job, new Path("/user/SparkToHBase"))

    //计算
    val rdd = sc.textFile("/user/user-logs-large.txt")
    val result = rdd.map(x => {
      val info = x.split("\\t")
      ((info(0), info(2)), 1)
    }).reduceByKey(_ + _)
    //转换成hbase格式
    val hbaseResult = result.map(x => {
      val put = new Put(Bytes.toBytes(x._1._1))
      put.addColumn(Bytes.toBytes("i"), Bytes.toBytes(x._1._2), Bytes.toBytes(x._2.toString))
      (new ImmutableBytesWritable, put)
    })

    //写入
    hbaseResult.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }


  def readFromHBase() = {
    val configuration = HBaseConfiguration.create(sc.hadoopConfiguration)
    //在configuration中配置hbase的所需参数
    configuration.set(TableInputFormat.INPUT_TABLE, "sparktest:user_ip_times")
    //      var job = Job.getInstance(configuration)
    //      job.setInputFormatClass()
    val hbaseRDD = sc.newAPIHadoopRDD(configuration
      , classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable]
      , classOf[Result])

    val resultRDD = hbaseRDD.map(x => {
      val result = x._2
      val user = Bytes.toString(result.getRow)
      val cellScanner = result.cellScanner()
      val values = new ArrayBuffer[String]()
      while(cellScanner.advance()){
        val cell = cellScanner.current()
        val ip = Bytes.toString(CellUtil.cloneQualifier(cell))
        val times = Bytes.toString(CellUtil.cloneValue(cell))
        values += s"$ip $times"
      }
      (user,values.toList)
    })

    resultRDD.foreach(println)
  }
  def readFromHBase01() = {
    val configuration = HBaseConfiguration.create(sc.hadoopConfiguration)
    //在configuration中配置hbase的所需参数
    configuration.set(TableInputFormat.INPUT_TABLE, "sparktest:user_ip_times")
    //      var job = Job.getInstance(configuration)
    //      job.setInputFormatClass()
    val hbaseRDD = sc.newAPIHadoopRDD(configuration
      , classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable]
      , classOf[Result])

    //对hbaseRDD重新转换, 让每个rdd的元素是一个单元格, 打印的结果和hbase的scan类似
    val cellRDD = hbaseRDD.flatMap(x => {
      val result = x._2
      val rowKey = Bytes.toString(result.getRow)
      val cellScanner = result.cellScanner()
      val output = new ArrayBuffer[String]()
      while(cellScanner.advance()){
        val cell = cellScanner.current()
        val ip = Bytes.toString(CellUtil.cloneQualifier(cell))
        val times = Bytes.toString(CellUtil.cloneValue(cell))
        val ts = cell.getTimestamp
        output += s"$rowKey ip:$ip times:$times timestamp:$ts"
      }
      output
    })

    cellRDD.foreach(println)
  }


  def main(args: Array[String]): Unit = {
//    writeToHBase()
//    readFromHBase()
    readFromHBase01()



  }


}
