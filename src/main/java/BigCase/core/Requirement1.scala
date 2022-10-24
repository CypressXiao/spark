package BigCase.core

import BigCase.beans.UserVisitAction
import BigCase.utils.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: BigCase.core
 * @objectName: Requirement1
 * @author: Cypress_Xiao
 * @description: 分别统计每个品类点击的次数，下单的次数和支付的次数
 * @date: 2022/7/2 21:51
 * @version: 1.0
 */
object Requirement1 {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtils.getSparkContext

    val dataRDD:RDD[String] = sc.textFile("C:\\Users\\Cypress_Xiao\\Desktop\\user_visit_action.txt")

    //过滤得到具有合理点击数的商品信息
    val splitRDD:RDD[UserVisitAction] = dataRDD.map(line => {
      val arr:Array[String] = line.split("_")
      try {
        UserVisitAction(arr(0), arr(1).toLong, arr(2), arr(3).toLong,
          arr(4), arr(5), arr(6).toLong, arr(7).toLong, arr(8), arr(9), arr(10), arr(11), arr(12).toLong)
      } catch {
        case exception:Exception => null
      }
    })

    //将公用数据缓存
    splitRDD.cache()

    /**
     * (品类,点击总数)
     */
    //根据商品类型进行分组
    val filterRDD:RDD[UserVisitAction] = splitRDD.filter(_ != null).filter(_.click_category_id != (-1))
    val clickGroupRDD:RDD[(Long, Iterable[UserVisitAction])] = filterRDD.groupBy(_.click_category_id)

    val res1:RDD[(String, Int)] = clickGroupRDD.map(e => {
      (e._1.toString, e._2.size) //品类及点击总次数
    })
    //res1.collect().foreach(println)

    println("***********************************************")

    /**
     * (品类,下单总数)
     */
    val orderIdRDD:RDD[String] = splitRDD.flatMap(e => {
      e.order_category_ids.split(",")
    })

    val orderGroupRDD:RDD[(String, Int)] = orderIdRDD.map(e => (e, 1))
    val res2:RDD[(String, Int)] = orderGroupRDD.reduceByKey(_ + _).filter(_._1 != "null")
    //res2.collect().foreach(println)

    println("***********************************************")

    /**
     * (品类,支付总数)
     */
    val payIdRDD:RDD[String] = splitRDD.flatMap(e => {
      e.pay_category_ids.split(",")
    })
    val tpRDD:RDD[(String, Int)] = payIdRDD.map(e => (e, 1))
    val payGroupRDD:RDD[(String, Iterable[Int])] = tpRDD.groupByKey()
    val res3:RDD[(String, Int)] = payGroupRDD.map(e => {
      (e._1, e._2.size)
    }).filter(_._1 != "null")

    //res3.foreach(println)

    /**
     * (品类,(点击总数,下单总数,支付总数)
     */
    val res4:RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = res1.cogroup(res2, res3)
    val res5:RDD[(String, (Int, Int, Int))] = res4.mapValues {
      case (e1, e2, e3) =>
        var cnt1 = 0;
        if (e1.iterator.hasNext) {
          cnt1 = e2.toIterator.next()
        }
        var cnt2 = 0;
        if (e2.toIterator.hasNext) {
          cnt2 = e2.toIterator.next()
        }
        var cnt3 = 0;
        if (e3.toIterator.hasNext) {
          cnt3 = e3.toIterator.next()
        }
        (cnt1, cnt2, cnt3)
    }
    val value:RDD[(String, (Int, Int, Int))] = res5.sortBy(e => e._2)
    value.foreach(println)

  }

  /**
   *自定义排序器
   */
  implicit def myRul:Ordering[(Int, Int, Int)] = new Ordering[(Int, Int, Int)] {
    override def compare(x:(Int, Int, Int), y:(Int, Int, Int)):Int = {
      if (x._1 == y._1) {
        if (x._2 == y._2) {
          x._3 - y._3
        } else {
          x._2 - y._2
        }
      } else {
        x._1 - y._1
      }
    }
  }
}
