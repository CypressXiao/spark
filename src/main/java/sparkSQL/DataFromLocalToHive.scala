package sparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: DataFromLocalToHive
 * @author: Cypress_Xiao
 * @description: 从本地加载文件生成视图表,然后将数据存储到hive里面
 * @date: 2022/7/13 21:57
 * @version: 1.0
 */
object DataFromLocalToHive {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val df:DataFrame = sess.read.json("file:///D:\\AllContent\\spark\\data\\stu.json")


  }

}
