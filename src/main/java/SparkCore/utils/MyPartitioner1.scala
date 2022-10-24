package SparkCore.utils

import org.apache.spark.Partitioner


/**
 * @projectName: spark 
 * @package: SparkCore.utils
 * @className: MyPartitioner1
 * @author: Cypress_Xiao
 * @description: 自定义迭代器1(根据城市分区)
 * @date: 2022/7/1 15:04
 * @version: 1.0
 */
class MyPartitioner1(arr:Array[String]) extends Partitioner{
  override def numPartitions:Int = arr.length

  override def getPartition(key:Any):Int = {
    val value:String = key.asInstanceOf[String]
    arr.indexOf(value)
  }
}
