package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: RDDtoDF_tp
 * @author: Cypress_Xiao
 * @description: RDD转化为DF
 * @date: 2022/7/11 17:08
 * @version: 1.0
 */
object RDDtoDF_tp {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkUtil.getSession("RDDtoDF")
    val sc:SparkContext = sess.sparkContext
    val dataRDD:RDD[String] = sc.textFile("file:///D:/AllContent/spark/data/log.log")
    val tpRDD:RDD[(Int, String, String, String, Int)] = dataRDD.map(line => {
      val arr:Array[String] = line.split(",")
      (arr(0).toInt, arr(1), arr(2), arr(3), arr(4).toInt)
    })


    val df:DataFrame = sess.createDataFrame(tpRDD)
    val df1:DataFrame = df.toDF("id", "name", "gender", "location", "age")
    df1.show()
    df1.printSchema()

    sess.close()
  }

}
