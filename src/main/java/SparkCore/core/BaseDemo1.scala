package SparkCore.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BaseDemo1 {

  //设置代码输出日志

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit = {
    //1.使用spark编程入口
    /*
    sparkConf 用于设置用户参数
    setMaster 设置运行模式 local 本地模式  [*]为默认核数
    setAppName 设置程序运行名
     */
    val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("BaseDemo")
    val context = new SparkContext(conf)

    //2.加载数据源
    val lines:RDD[String] = context.textFile("C:\\Users\\Cypress_Xiao\\Desktop\\data")

    //3.计算逻辑
    val arr:RDD[String] = lines.map(_ + "6")

    //4.输出 需要行动算子,其他的是转换算子
    arr.foreach(println)
    //5.关闭
    context.stop()

  }
}
