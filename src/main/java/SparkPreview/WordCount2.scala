package SparkPreview

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCount2 {
  def main(args:Array[String]):Unit = {
    val conf:SparkConf = new SparkConf().setMaster("local").setAppName("WordCount1")
    val sc = new SparkContext(conf)

    val lines:RDD[String] = sc.textFile("C:\\Users\\Cypress_Xiao\\Desktop\\data")
    val arr1:RDD[(String, Int)] = lines.flatMap(e => {
      val arr:Array[String] = e.split("\\s+")
      arr.map(e => (e, 1))
    })

    arr1.reduceByKey((x,y)=>x+y).collect().foreach(println)


  }

}
