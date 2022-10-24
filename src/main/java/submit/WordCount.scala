package submit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @projectName: spark 
 * @package: submit
 * @objectName: WordCount
 * @author: Cypress_Xiao
 * @description: 集群上统计单词个数
 * @date: 2022/7/8 10:51
 * @version: 1.0
 */

object WordCount {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val res:RDD[(String, Int)] = sc.textFile("hdfs://hadoop001:8020/spark/wordcount")
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)
    res.foreach(println)

    //res.saveAsTextFile("hdfs://hadoop001:8020/sparkoutput")
    Thread.sleep(1000 * 60 * 100)
    sc.stop()

  }

}
