package SparkCore.core

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: Transformations_Operator_LoadToMysql
 * @author: Cypress_Xiao
 * @description: 将文件中获取的数据导入mysql表中
 * @date: 2022/6/29 17:57
 * @version: 1.0
 */

object Transformations_Operator_LoadToMysql {
  def main(args:Array[String]):Unit = {
    val conf:SparkConf = new SparkConf().setMaster("local[3]").setAppName("dataToMysql")
    val sc = new SparkContext(conf)

    val rdd1:RDD[String] = sc.textFile("data/score.csv")

    //首先下面传入对象时需要序列化,但是下面这个方法和数据库建立的连接太多了
    /*rdd1.foreach(line=>{
      //要写入mysql不得先获取mysql连接么
      val conn:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/my_db1", "root", "123456")
      //创建一个写sql语句的对象
      val ps:PreparedStatement = conn.prepareStatement("insert into tb_score values(?,?,?,?)")
      val arr:Array[String] = line.split(",")
      ps.setInt(1,arr(0).toInt)
      ps.setString(2,arr(1))
      ps.setString(3,arr(2))
      ps.setDouble(4,arr(3).toDouble)
      ps.executeUpdate()
    })*/

    rdd1.foreachPartition(iters =>{
      //要写入mysql不得先获取mysql连接么
      val conn:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/my_db1", "root", "123456")
      //创建一个写sql语句的对象
      val ps:PreparedStatement = conn.prepareStatement("insert into tb_score values(?,?,?,?)")
      iters.foreach(line=>{
        val arr:Array[String] = line.split(",")
        ps.setInt(1,arr(0).toInt)
        ps.setString(2,arr(1))
        ps.setString(3,arr(2))
        ps.setDouble(4,arr(3).toDouble)
        ps.executeUpdate()
      })
    })
  }
}
