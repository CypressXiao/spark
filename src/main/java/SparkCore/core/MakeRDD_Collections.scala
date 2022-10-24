package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: SparkDay01.core
 * @objectName: MakeRDD_Collections
 * @author: Cypress_Xiao
 * @description: 测试使用,本地集合转换为RDD 本地集合必须是seq及其子集,map不能直接创建
 * @date: 2022/6/28 17:19
 * @version: 1.0
 */

object MakeRDD_Collections {
  def main(args:Array[String]):Unit = {
    val arr:Array[Int] = Array[Int](1, 2, 3, 4, 5)
    val ls:_root_.scala.collection.immutable.List[_root_.scala.Predef.String] = List[String]("Jordan", "James", "Kobe", "Wade")
    val mp:Map[String, Int] = Map[String, Int]("zss" -> 23, "lss" -> 34)
    val mpToList:List[(String, Int)] = mp.toList
    val sc:SparkContext = SparkUtil.getSparkContext("mkRDD_Collections")
    val rdd1:RDD[Int] = sc.makeRDD(arr)
    val rdd2:RDD[String] = sc.makeRDD(ls)
    val rdd3:RDD[(String, Int)] = sc.makeRDD(mpToList)

    //处理数据 转换算子(从一个RDD转化为另一个RDD)
    val resRDD1:RDD[Int] = rdd1.map(_ * 10)
    //println(resRDD1)

    resRDD1.foreach(println)
    sc.stop()

  }
}
