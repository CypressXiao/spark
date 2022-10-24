package SparkCore.core

import java.sql.{Connection, DriverManager, ResultSet}

import SparkCore.beans.UserBean
import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * @projectName: spark
 * @package: SparkCore.core
 * @objectName: MakeRDD_JDBC
 * @author: Cypress_Xiao
 * @description: mysql生成RDD
 * @date: 2022/6/29 9:19
 * @version: 1.0
 */

object MakeRDD_JDBC {

  //生成mysql连接对象
   val getConnection:() => Connection = () => {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/my_db1", "root", "123456")
  }


  val rs:ResultSet => UserBean = (res:ResultSet) => {
    //根据获取的列数进行封装,列数要和mysql中的列数匹配
    val id:Int = res.getInt(1)
    val name:String = res.getString(2)
    val key:String = res.getString(3)
    val image:String = res.getString(4)
    val birth:String = res.getString(5)
    //封装为自己想要的bean对象
    UserBean(id, name, key, image, birth)
  }

  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("mk_RDD_mysql")

    /**
     * sc:spark环境
     * getConnection:一个函数,用于与mysql建立连接
     * lowerBound:读取的数据的开始行
     * upperBound:读取数据的末尾行
     * numPartitions:分区数
     * rs:将查询到的数据进行封装成bean对象
     */

    val rdd1 = new JdbcRDD[UserBean](sc,
      getConnection,
      "select * from user where id>=? and id<=?",
      1,
      3,
      1,
      rs
    )
    rdd1.foreach(println)


  }

}
