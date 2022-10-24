package SparkCore.core

import SparkCore.classes.MyAccumulator
import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: My累加器
 * @author: Cypress_Xiao
 * @description: 测试自定义累加器类
 * @date: 2022/7/4 17:10
 * @version: 1.0
 */
object My累加器 {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("自定义累计器")
    val myAcc = new MyAccumulator
    //是用自定义累加器之前要先注册
    sc.register(myAcc)
    val dataRDD:RDD[String] = sc.textFile("data/wordcount")
    val words:RDD[String] = dataRDD.flatMap(_.split("\\s+"))
    words.foreach(word => {
      //遍历添加个数
      myAcc.add(word)
    })
    //获取包含需要的数据的结果
    val resMp:mutable.HashMap[String, Int] = myAcc.value
    resMp.foreach(println)


  }

}
