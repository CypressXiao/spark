package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: Collections_Partitions_Data
 * @author: Cypress_Xiao
 * @description: TODO  
 * @date: 2022/6/29 11:48
 * @version: 1.0
 */
object Collections_Partitions_Data {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("RDD分区数")

    val ls:_root_.scala.collection.immutable.List[Int] = List[Int](1,2,3,4,5,6,7)
    val rdd:RDD[Int] = sc.makeRDD(ls,3)

    var index = 0;
    val rdd1:RDD[String] = rdd.mapPartitions(iters => {
      index += 1 //这样实现不了,因为我会将相同的逻辑封装到rdd中,每个分区获取的index是相同的
      iters.map(e => {
        e + "--" + index
      })
    })

    println(index)


    //以数据分区为单位处理数据,s为分区编号
    /*rdd.mapPartitionsWithIndex((s,iters) =>{
      iters.map(e=>(e,s))
    }).foreach(println)*/



  }

}
