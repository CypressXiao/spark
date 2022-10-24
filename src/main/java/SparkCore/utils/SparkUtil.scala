package SparkCore.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {
  //提前设置日志输出格式
  Logger.getLogger("org").setLevel(Level.ERROR)

  def getSparkContext(appName:String):SparkContext ={
    val conf:SparkConf = new SparkConf().setMaster("local").setAppName(appName)
    new SparkContext(conf)
  }

  def getSparkContext(appName:String,cores:Int):SparkContext ={
    val conf:SparkConf = new SparkConf().setMaster(s"local[$cores]").setAppName(appName)
    new SparkContext(conf)
  }

  def getSession(appName:String,num:Int):SparkSession ={
    SparkSession.builder().appName(appName).master(s"local[${num}]").getOrCreate()
  }

  def getSession(appName:String):SparkSession ={
    SparkSession.builder().appName(appName).master(s"local[*]").getOrCreate()
  }

}
