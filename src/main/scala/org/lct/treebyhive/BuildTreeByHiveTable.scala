package org.lct.treebyhive

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object BuildTreeByHiveTable {
  var spark: SparkSession = null
  var level = 1

  def main(args: Array[String]): Unit = {
    spark = SparkSession.builder()
      .master("local")
      .appName("test01")
      .getOrCreate()
    val df01: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://49.232.128.168:3306/test")
      .option("dbtable", "tb01")
      .option("user", "root")
      .option("password", "lctlj")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    val arrbuf_all = new ArrayBuffer[ArrayBuffer[String]]()
    val stock_id_list = new ArrayBuffer[String]()
    stock_id_list.append("s1", "s2")
    stock_id_list.foreach(println)
    df01.createOrReplaceTempView("tb01")
    getChild(stock_id_list, arrbuf_all)
    println(arrbuf_all.size)
    for (ele <- arrbuf_all) {
      for (ele01 <- ele) {
        println(ele01)
      }
    }
    spark.stop()
  }

  def getChild(stock_id_list: ArrayBuffer[String], arrbuf_all: ArrayBuffer[ArrayBuffer[String]]): Unit = {

    level = level + 1
    for (stock_id <- stock_id_list) {
      println("stock_id:" + stock_id)
      //      添加顶级节点
      if ((level - 1) == 1) {
        val tmp_buf = new ArrayBuffer[String]()
        val tmp = "empty" + "##" + stock_id + "##" + 0 + "##" + (level - 1)
        tmp_buf.append(tmp)
        arrbuf_all.append(tmp_buf)
      }

      val tmp_buf_path = new ArrayBuffer[String]()
      val tmp_buf_sid_all = new ArrayBuffer[String]()
      //      筛选大于等于30%
      val rows_gt30: Array[Row] = spark.sql(s"""select eid,stock_id,stock_percent from tb01 where stock_id = '$stock_id' and stock_percent >= 30 and eid is not null and eid<>''""").collect()
      if (!rows_gt30.isEmpty) {
        var tmp = ""
        for (ele01 <- rows_gt30) {
          val eid = ele01.get(0).toString
          val stock_id = ele01.get(1).toString
          val stock_percent = ele01.get(2).toString
          tmp = stock_id + "##" + eid + "##" + stock_percent + "##" + level
          //        临时存放当前层的节点
          tmp_buf_sid_all.append(eid)
          //          路径和层级
          tmp_buf_path.append(tmp)

        }
      }
      //      筛选小于30的
      //  实际环境：select eid,stock_id,stock_percent from (select eid,stock_id,stock_percent,rank() over(partition by eid order by stock_percent desc) rank from tb01 where stock_id = 's1' and stock_percent < 30 and eid is not null and eid !='') t1 where ran=1
      val rows_lt29: Array[Row] = spark.sql(s"""select eid,stock_id,stock_percent from tb01 where stock_id = '$stock_id' and stock_percent < 30 and eid is not null and eid<>''""").collect()
      if (!rows_lt29.isEmpty) {
        var tmp = ""
        for (ele01 <- rows_lt29) {
          val eid = ele01.get(0).toString
          val stock_id = ele01.get(1).toString
          val stock_percent = ele01.get(2).toString
          tmp = stock_id + "##" + eid + "##" + stock_percent + "##" + level
          //        临时存放当前层的节点
          tmp_buf_sid_all.append(eid)
          //          路径和层级
          tmp_buf_path.append(tmp)
        }
      }

      if (!tmp_buf_sid_all.isEmpty) {
        arrbuf_all.append(tmp_buf_path)
        getChild(tmp_buf_sid_all, arrbuf_all)
        level -= 1
      }

      if (tmp_buf_sid_all.isEmpty) {
        val tmp_buf = new ArrayBuffer[String]()
        val tmp = stock_id + "##" + "empty" + "##" + 0 + "##" + (level - 1)
        tmp_buf.append(tmp)
        arrbuf_all.append(tmp_buf)
      }
    }
}

}
