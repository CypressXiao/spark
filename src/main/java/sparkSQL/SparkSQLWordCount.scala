package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: SparkSQLWordCount
 * @author: Cypress_Xiao
 * @description: SparkSQL单词统计
 * @date: 2022/7/11 17:32
 * @version: 1.0
 */
object SparkSQLWordCount {
  def main(args:Array[String]):Unit = {

    DFWordCount

  }

  //dataFrame统计单词次数
  private def DFWordCount:Unit = {
    val sess:SparkSession = SparkUtil.getSession(this.getClass.getSimpleName)
    //必须导入隐式转换相关的包
    import sess.implicits._
    import org.apache.spark.sql.functions._

    val sc:SparkContext = sess.sparkContext
    val data:RDD[String] = sc.textFile("file:///D:\\AllContent\\spark\\data\\wordcount")
    val rdd1:RDD[String] = data.flatMap(_.split("\\s+"))
    val df:DataFrame = rdd1.toDF("word") //因为我只有一列,所以我的列名就一个

    df.groupBy("word").count().show()
  }

  //RDD统计单词次数
  private def RDDWordCount():Unit = {
    val sess:SparkSession = SparkUtil.getSession("wordCount")
    val sc:SparkContext = sess.sparkContext
    val dataRDD:RDD[String] = sc.textFile("hdfs://hadoop001:8020/spark/wordcount")
    val rdd1:RDD[(String, Int)] = dataRDD.flatMap(line => {
      line.split("\\s+").map((_, 1))
    })
    val res:RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
    res.foreach(println)
  }
}
