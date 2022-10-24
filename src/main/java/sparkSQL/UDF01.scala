package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL.udf
 * @objectName: UDF01
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/7/14 9:34
 * @version: 1.0
 */
object UDF01 {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkUtil.getSession(this.getClass.getSimpleName)
    val df:DataFrame = sess.read.option("header", value = true).csv("file:///D:\\AllContent\\spark\\data\\score1.csv")
    df.createTempView("tb_score")
    sess.udf.register("info",info1 _)

    sess.sql(
      """
        |select
        |id,info(name) as name
        |from
        |tb_score
        |""".stripMargin).show()
  }

  val info:String => String = (name:String) =>{
    "帅"+name
  }

  def info1(name:String):String ={
    "handsome"+name
  }

}
