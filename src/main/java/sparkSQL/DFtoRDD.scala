package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: DFtoRDD
 * @author: Cypress_Xiao
 * @description: dataFrame转化为RDD进行计算
 * @date: 2022/7/13 16:08
 * @version: 1.0
 */
object DFtoRDD {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkUtil.getSession(this.getClass.getSimpleName)
    val df:DataFrame = sess.read.option("inferSchema", value = true).option("header", value = true)
      .csv("file:///D:\\AllContent\\spark\\data\\score1.csv")
    /**
     * 将dataFrame转化为RDD
     */
    val dataRDD:RDD[Row] = df.rdd
    val res:_root_.org.apache.spark.rdd.RDD[(Int, _root_.scala.Predef.String, _root_.scala.Predef.String, Int)] = getDataFromRDD3(dataRDD)

    res.foreach(println)
  }


  //解析Row方式3 模式匹配:Row()
  private def getDataFromRDD3(dataRDD:RDD[Row]):RDD[(Int, String, String, Int)] = {
    val res:RDD[(Int, String, String, Int)] = dataRDD.map({
      case Row(id:Int, name:String, sub:String, score:Int) => {
        (id, name, sub, score)
      }
    })
    res
  }

  //解析Row方式2 getString(col数字)
  private def getDataFromRDD2(dataRDD:RDD[Row]):RDD[(Int, String, String, Int)] = {
    val res:RDD[(Int, String, String, Int)] = dataRDD.map(data => {
      val id:Int = data.getInt(0)
      val name:String = data.getString(1)
      val sub:String = data.getString(2)
      val score:Int = data.getInt(3)
      (id, name, sub, score)
    })
    res
  }

  /**
   * @param dataRDD : dataFrame转化来的RDD
   * @return RDD<Tuple2<String,Object>>
   * @author Cypress_Xiao
   * @description 从转化来的RDD中获取数据的方式1
   * @date 2022/7/13 16:27
   */
  private def getDataFromRDD1(dataRDD:RDD[Row]):RDD[(String, Double)] = {
    //求每个学生的平均成绩
    val rdd1:RDD[(String, (Int, String, String, Int))] = dataRDD.map(row => {
      /**
       * 从转化来的RDD中取数据的方式1(即解析ROW中数据)
       */
      val id:Int = row.getAs[Int]("id")
      val name:String = row.getAs[String]("name")
      val sub:String = row.getAs[String]("sub")
      val score:Int = row.getAs[Int]("score")
      (name, (id, name, sub, score))
    })

    val rdd2:RDD[(String, Iterable[(Int, String, String, Int)])] = rdd1.groupByKey()

    val res:RDD[(String, Double)] = rdd2.map(data => {
      (data._1, data._2.map(_._4).sum / data._2.size)
    })
    res
  }
}
