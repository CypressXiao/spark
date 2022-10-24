package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LoadHDFS {
  def main(args:Array[String]):Unit = {

    val context:SparkContext = SparkUtil.getSparkContext("SCORE_COUNT")

    val lines:RDD[String] = context.textFile("hdfs://hadoop001:8020/user/hive/warehouse/doit32.db/grade")


    lines
      .map(line =>{
      val arr:Array[String] = line.split(",")
      (arr(0),arr(2))
    })
      .groupBy(_._1)
      .map(e=>{
        (e._1,e._2.map(_._2.toInt).sum)
      })
      .sortBy(e=>e._2,ascending = false)
      .foreach(println)
  }
}
