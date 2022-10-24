package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: RDDtoDF_ScalaBean
 * @author: Cypress_Xiao
 * @description: TODO  
 * @date: 2022/7/11 17:25
 * @version: 1.0
 */

case class Per(id:Int,name:String,gender:String,location:String,age:Int)

object RDDtoDF_ScalaBean {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkUtil.getSession("RDDtoDF")
    val sc:SparkContext = sess.sparkContext
    val dataRDD:RDD[String] = sc.textFile("file:///D:/AllContent/spark/data/log.log")
    val caseClassRDD:RDD[Per] = dataRDD.map(line => {
      val arr:Array[String] = line.split(",")
      Per(arr(0).toInt, arr(1), arr(2), arr(3), arr(4).toInt)
    })


    val df:DataFrame = sess.createDataFrame(caseClassRDD)
    df.show()
    df.printSchema()

    sess.close()
  }

}
