package com.zhiyou.bd14

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlTest {


  val conf = new SparkConf().setMaster("local[*]")
    .setAppName("spark读写mysql")
  val sc = SparkContext.getOrCreate(conf)


  val url = "jdbc:mysql://localhost:3306/bigdata14"
  val username = "root"
  val password = "root"


  val getMysqlConnection = () => {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection(url, username, password)
  }

  //从mysql读取数据
  def readFromMysql() = {
    val sql = "select * from department where id > ? and id < ?"
    val mysqlRDD = new JdbcRDD(sc, getMysqlConnection, sql, 1, 100, 2, x => x)

    mysqlRDD.foreach(x => {
      println(s"id:${x.getInt("id")}, name:${x.getString("name")}")
    })

  }


  //写入mysql
  def writeToMysql() = {
    //统计每个用户在每个ip上的行为次数, 结果存放到mysql表user_ip_times
    val sql = ""
    val rdd = sc.textFile("/user/user-logs-large.txt")
    val result = rdd.map(x => {
      val info = x.split("\\t")
      ((info(0), info(2)), 1)
    }).reduceByKey(_ + _)

    //把rdd的数据存放到mysql上
    result.foreachPartition(x => {
      //批量写入
      val connection = getMysqlConnection()
      val sql = "insert into user_ip_times (userName, ip, times) values (?,?,?)"
      val preparedStatement = connection.prepareStatement(sql)
      x.foreach(record => {
        //写入
        preparedStatement.setString(1, record._1._1)
        preparedStatement.setString(2, record._1._2)
        preparedStatement.setInt(3, record._2)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      connection.close()
    })
  }


  def main(args: Array[String]): Unit = {
//    readFromMysql()
    writeToMysql()
  }


}


//统计出每个用户的订单数, 消费总金额, 购买过的产品类型的数量
//                   如果一个用户, 总共下过2个订单, 第一个订单里面买了手机 男装
//                                                第二个订单里面买了手机 女装
//                                                购买的产品的类型数量是 3
//                                                2. 统计每个店铺销售的商品品类的数
//                                                量, 总销售额
