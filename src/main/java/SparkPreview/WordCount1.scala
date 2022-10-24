package SparkPreview

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount1 {
  def main(args:Array[String]):Unit = {
    val conf:SparkConf = new SparkConf().setMaster("local").setAppName("WordCount1")
    val sc = new SparkContext(conf)

    val lines:RDD[String] = sc.textFile("C:\\Users\\Cypress_Xiao\\Desktop\\data")
    val arr1:RDD[(String, Int)] = lines.flatMap(e => {
      val arr:Array[String] = e.split("\\s+")
      arr.map(e => (e, 1))
    })
    val arr2:RDD[(String, Iterable[(String, Int)])] = arr1.groupBy(e => e._1)

    val res1:RDD[(String, Int)] = arr2.map(e => {
      (e._1, e._2.map(e => e._2).sum)
    })
    res1.collect().foreach(println)

    sc.stop()


  }

}
