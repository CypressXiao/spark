package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: UDAF01
 * @author: Cypress_Xiao
 * @description: TODO  
 * @date: 2022/7/14 11:02
 * @version: 1.0
 */
object UDAF01 {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkUtil.getSession(this.getClass.getSimpleName)
    val df:DataFrame = sess.read.option("header", value = true).csv("file:///D:\\AllContent\\spark\\data\\score1.csv")
    df.createTempView("tb_score")
    import org.apache.spark.sql.functions._
    sess.udf.register("myAvg",udaf(new MyAvg))

    sess.sql(
      """
        |select
        |myAvg(score) as avg_score
        |from
        |tb_score
        |""".stripMargin).show()


  }

  case class midRes(sum:Double, cnt:Int)

  class MyAvg extends Aggregator[Double, midRes, Double] {
    override def zero:midRes = midRes(0, 0)

    override def reduce(b:midRes, a:Double):midRes = {
      midRes(b.sum + a, b.cnt + 1)
    }

    override def merge(b1:midRes, b2:midRes):midRes = {
      midRes(b1.sum+b2.sum,b1.cnt+b2.cnt)
    }

    override def finish(reduction:midRes):Double = reduction.sum/reduction.cnt

    override def bufferEncoder:Encoder[midRes] = Encoders.product

    override def outputEncoder:Encoder[Double] = Encoders.scalaDouble
  }

}
