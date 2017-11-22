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


  def main(args: Array[String]): Unit = {
    readFromMysql()
  }


}


//统计出每个用户的订单数, 消费总金额, 购买过的产品类型的数量
//                   如果一个用户, 总共下过2个订单, 第一个订单里面买了手机 男装
//                                                第二个订单里面买了手机 女装
//                                                购买的产品的类型数量是 3
//                                                2. 统计每个店铺销售的商品品类的数
//                                                量, 总销售额
