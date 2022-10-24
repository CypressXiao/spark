package SparkCore.core

import SparkCore.beans.RDDUserBean
import SparkCore.utils.SparkUtil
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: _统计脏数据条数
 * @author: Cypress_Xiao
 * @description: TODO  
 * @date: 2022/7/4 15:55
 * @version: 1.0
 */
object _统计脏数据条数 {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("统计脏数据条数")

    val cnt:LongAccumulator = sc.longAccumulator("脏数据个数")

    val dataRDD:RDD[String] = sc.textFile("data/user")
    val res:Unit = dataRDD.foreach(line => {
      val bean:RDDUserBean = try {
        JSON.parseObject(line, classOf[RDDUserBean])
      } catch {
        case exception:Exception => {
          null
        }
      }
      if(bean == null){
        cnt.add(1)
      }
    })


    println(cnt.value)

    sc.stop()


  }

}
