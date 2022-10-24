package SparkCore.utils

import org.apache.spark.Partitioner

/**
 * @projectName: spark 
 * @package: SparkCore.utils
 * @className: MyPartitions
 * @author: Cypress_Xiao
 * @description: 自定义分区器
 * @date: 2022/7/1 14:34
 * @version: 1.0
 */
class MyPartitions extends Partitioner{
  override def numPartitions:Int = 2

  override def getPartition(key:Any):Int = {
    val value:Int = key.asInstanceOf[Int]
    if(value <= 2){
      0
    }else{
      1
    }
  }


}
