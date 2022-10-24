package SparkPreview

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args:Array[String]):Unit = {
    //配置对象
    val conf:SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")

    //建立和spark框架的连接
    val context = new SparkContext(conf)

    //1.读取文件获取数据
    val lines:RDD[String] = context.textFile("C:\\Users\\Cypress_Xiao\\Desktop\\data")

    //2.将一行数据进行拆分,形成一个个的单词
    val words:RDD[String] = lines.flatMap(_.split("\\s+"))

    //3.将数据根据单词分组
    val group:RDD[(String, Iterable[String])] = words.groupBy(e => e)

    //4.数据统计
    val res:RDD[(String, Int)] = group.map(e => {
      (e._1, e._2.size)
    })

    //5.采集数据,显示数据
    val res1:Array[(String, Int)] = res.collect()
    res1.foreach(println)

    //关闭连接
    context.stop()

  }

}
