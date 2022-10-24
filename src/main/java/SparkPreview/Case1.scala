package SparkPreview

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable

/**
 * @projectName: spark 
 * @package: SparkPreview
 * @objectName: Case1
 * @author: Cypress_Xiao
 * @description: 统计每一个省份每个广告被点击的数量的top3
 * @date: 2022/6/28 21:09
 * @version: 1.0
 */

object Case1 {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("TOP3")
    val lines:RDD[String] = sc.textFile("C:\\Users\\Cypress_Xiao\\Desktop\\agent.log")

    lines
      .map(_.split("\\s+"))
      .groupBy(_ (1))
      .flatMap(tp => {
        val ad:Map[String, Iterable[Array[String]]] = tp._2.groupBy(_ (4))
        val tuples:immutable.Iterable[(String, String, Int)] = ad.map(e => {
          (tp._1, e._1, e._2.size)
        })
        tuples
      })
      .groupBy(_._1)
      .flatMap(e => {
        val res:List[(String, String, Int)] = e._2.toList.sortBy(_._3).reverse
        val tuples:List[(String, String, Int)] = res.take(3)
        tuples
      })
      .collect()
      .sorted
      .foreach(println)


  }
/**
 * @param: 不需传参
 * @return Ordering<Tuple3<String,String,Object>>
 * @author Cypress_Xiao
 * @description 定义新的排序规则
 * @date 2022/6/29 9:13
 */
  implicit def orderRule:Ordering[(String, String, Int)] = new Ordering[(String, String, Int)] {
    override def compare(x:(String, String, Int), y:(String, String, Int)):Int = {
      if (x._1 == y._1) {
        y._3 - x._3
      }
      else x._1.compareTo(y._1)
    }
  }
}
