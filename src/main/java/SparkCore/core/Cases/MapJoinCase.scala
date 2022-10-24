package SparkCore.core.Cases


import SparkCore.beans.City
import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.{BufferedSource, Source}

/**
 * @projectName: spark 
 * @package: SparkCore.core.Cases
 * @objectName: MapJoinCase
 * @author: Cypress_Xiao
 * @description: map端join小案例
 * @date: 2022/7/2 16:10
 * @version: 1.0
 */

object MapJoinCase {
  def main(args:Array[String]):Unit = {
    //先将小文件提前读取,存储在集合(最好是静态的)中,集合中的类的也要能序列化,避免join时的shuffle,文件内容较小时不用spark环境读取
    val cities:BufferedSource = Source.fromFile("data/join/city.txt")
    val mp:Map[String, City] = cities.getLines().map(line => {
      val arr:Array[String] = line.split(",")
      val city:City = City(arr(0), arr(1))
      (arr(0), city)
    }).toMap

    val sc:SparkContext = SparkUtil.getSparkContext("mapJoin")
    val dataRDD:RDD[String] = sc.textFile("data/join/User.txt")
    val res:RDD[(String, String, String, String)] = dataRDD.map(line => {
      val arr:Array[String] = line.split(",")
      val city:City = mp.getOrElse(arr(2), null)
      try {
        (arr(0), arr(1), arr(2), city.cname)
      } catch {
        case _: Exception => (arr(0),arr(1),arr(2),null)
      }
    })
    res.foreach(println)
  }

}
