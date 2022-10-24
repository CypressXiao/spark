package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: UDAF03
 * @author: Cypress_Xiao
 * @description: TODO  
 * @date: 2022/7/14 16:32
 * @version: 1.0
 */
object UDAF03 {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkUtil.getSession(this.getClass.getSimpleName)

    val df:DataFrame = sess.read.option("header", value = true)
      .csv("file:///D:\\AllContent\\spark\\data\\score1.csv")

    df.createTempView("tb_top2")

    import org.apache.spark.sql.functions._
    sess.udf.register("top2",udaf(new top2))


    val df2:DataFrame = sess.sql(
      """
        |select
        |sub,
        |top2(score) as res
        |from
        |tb_top2
        |group by sub
        |""".stripMargin)

    df2.show()
  }
}

case class Buffer(max:Double, sec:Double)

class top2 extends Aggregator[Double, Buffer, String] {
  override def zero:Buffer = Buffer(0, 0)

  override def reduce(b:Buffer, a:Double):Buffer = {
    val list:_root_.scala.collection.immutable.List[Double] = List[Double](b.max, b.sec, a).sortBy(e => -e)
    Buffer(list.head, list(1))
  }

  override def merge(b1:Buffer, b2:Buffer):Buffer = {
    val l:_root_.scala.collection.immutable.List[Double] = List[Double](b1.max, b1.sec, b2.max, b2.sec).sortBy(-_)
    Buffer(l.head, l(1))
  }

  override def finish(reduction:Buffer):String = s"{${reduction.max}_${reduction.sec}"

  override def bufferEncoder:Encoder[Buffer] = Encoders.product

  override def outputEncoder:Encoder[String] = Encoders.STRING
}
