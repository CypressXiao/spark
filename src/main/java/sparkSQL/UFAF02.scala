package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @className: UFAF02
 * @author: Cypress_Xiao
 * @description: TODO  
 * @date: 2022/7/14 14:18
 * @version: 1.0
 */
object UFAF02 {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkUtil.getSession(this.getClass.getSimpleName)
    val df:DataFrame = sess.read.option("inferSchema",value = true).option("header", value = true)
      .csv("file:///D:\\AllContent\\spark\\data\\feature.csv")
    import sess.implicits._
    import org.apache.spark.sql.functions._
    df.createTempView("tb_feature")

    val df1:DataFrame = sess.sql(
      """
        |select
        |id,
        |name,
        |array(age,height,weight,yanzhi,score) as features
        |from tb_feature
        |""".stripMargin)

    val df2:DataFrame = df1.join(df1.toDF("id2", "name2", "features2"), 'id < 'id2)
    df2.createTempView("tb_join")

    def familiar(arr:Array[Double], arr1:Array[Double]):Double = {
      val fm1:Double = Math.pow(arr.map(e => e * e).sum, 0.5)
      val fm2:Double = Math.pow(arr1.map(e => e * e).sum, 0.5)
      val tp:Array[(Double, Double)] = arr.zip(arr1)
      val fz:Double = tp.map(t => t._1 * t._2).sum
      fz / (fm1 * fm2)
    }
    sess.udf.register("familiar",familiar _)

    sess.sql(
      """
        |select
        |id,
        |name,
        |id2,
        |name2,
        |familiar(features,features2) as familiar
        |from
        |tb_join
        |""".stripMargin).show()

  }

}
