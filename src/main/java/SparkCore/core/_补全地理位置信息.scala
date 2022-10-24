package SparkCore.core

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}

import SparkCore.utils.SparkUtil
import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{JdbcRDD, RDD}

import scala.collection.mutable


/**
 * @projectName: spark
 * @package: SparkCore.core
 * @objectName: _补全地理位置信息
 * @author: Cypress_Xiao
 * @description: 补全地理位置的测试类
 * @date: 2022/7/4 9:44
 * @version: 1.0
 */
object _补全地理位置信息 {
  def main(args:Array[String]):Unit = {
    /**
     * 1.建立spark环境
     * 2.从文件中获取数据
     * 3.获取数据后先看数据库或者hdfs中有无位置数据,有则返回,无则进行第四步操作
     * 4.去网络端获取地理位置,获取后输出,并在hdfs或者mysql中存储
     */

    //1.建立spark环境
    val sc:SparkContext = SparkUtil.getSparkContext("补全地理位置信息", 8)

    //2.从文件中获取需要处理的数据
    var dataRDD:RDD[String] = sc.textFile("data\\latitude&&longitude")
    //3.从mysql中读取数据
    //(1).连接数据库
    val conn:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/my_db1", "root", "123456")
    val mp = new mutable.HashMap[String, (String, String, String)]()
    getMp(conn, mp)
    //4.看rdd中的数据有无在mp中,注意序列化问题
    val res:RDD[(String, String, String)] = dataRDD.mapPartitions(iter => {
      val conn1:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/my_db1", "root", "123456")
      iter.map(line => {
        val arr:Array[String] = line.split(",")
        val geo:String = GeoHash.withCharacterPrecision(arr(2).toDouble, arr(1).toDouble, 8).toBase32
        if (mp.contains(geo)) {
          //如果在mysql中直接取值
          val tp = mp.getOrElse(geo, (null, null, null))
          (tp._1, tp._2, tp._3)
        } else {
          val tp1:(String, String, String) = getLInfo(arr(1).toDouble, arr(2).toDouble)
          insertToTab(geo, conn1, arr, tp1)
          (tp1._1, tp1._2, tp1._3)
        }
      })
    })
    res.foreach(println)
  }


  private def insertToTab(geo:String, conn:Connection, arr:Array[String], tp:(String, String, String)):Int = {
    //获得新数据输出的时候需要存到mysql
    val ps:PreparedStatement = conn.prepareStatement("insert into location_info values (?,?,?,?,?,?)")
    ps.setString(1, geo)
    ps.setDouble(2, arr(1).toDouble)
    ps.setDouble(3, arr(2).toDouble)
    ps.setString(4, tp._1)
    ps.setString(5, tp._2)
    ps.setString(6, tp._3)
    ps.executeUpdate()
  }

  private def getMp(conn:Connection, mp:mutable.HashMap[String, (String, String, String)]):Unit = {
    val statement:Statement = conn.createStatement()
    val resultSet:ResultSet = statement.executeQuery("select * from location_info")
    //(2)将获得的数据存储到一个hashmap中
    while (resultSet.next()) {
      val s1:String = resultSet.getString("province")
      val s2:String = resultSet.getString("city")
      val s3:String = resultSet.getString("district")
      val s4:String = resultSet.getString("geoHash")
      mp.put(s4, (s1, s2, s3))
    }
  }

  def getLInfo(lo:Double, la:Double):(String, String, String) = {
    //1.根据传入的经纬度,拼接成url
    val url = s"https://restapi.amap.com/v3/geocode/regeo?output=json&location=${lo},${la}&key=e6a09975863c519ddbc03e5c47108367"
    //2.根据url请求连接网络获取数据
    //(1)创建httpclient对象
    val client:CloseableHttpClient = HttpClients.createDefault
    //(2)创建http方法对象
    val get = new HttpGet(url)
    //(3)发送请求获取相应的数据
    val response:CloseableHttpResponse = client.execute(get)
    //(4)获取对象实例并将其转化为json
    val entity:HttpEntity = response.getEntity
    val data:String = EntityUtils.toString(entity, "utf-8")
    val resObject:JSONObject = JSON.parseObject(data).getJSONObject("regeocode").getJSONObject("addressComponent")
    //(5)获得需要的省市信息
    (resObject.getString("province"), resObject.getString("city"), resObject.getString("district"))
  }
}
