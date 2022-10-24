package SparkCore.core

import SparkCore.beans.{RDDUserBean, Student}
import SparkCore.utils.{MyPartitioner1, MyPartitions, SparkUtil}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD



/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @className: TransformationDemo1
 * @author: Cypress_Xiao
 * @description: 转换算子
 * @date: 2022/7/1 10:01
 * @version: 1.0
 */
object TransformationDemo1 {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("转换算子")
    val rdd1:RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 4, 5), 3)
    val rdd2:RDD[Int] = sc.parallelize(List(4, 4, 5, 6, 7, 8), 3)
    val rdd3:RDD[String] = sc.parallelize(Array("java", "go", "spark", "hive", "FLink"), 3)
    


  }

  private def myPartitioner1(sc:SparkContext):Unit = {
    val lines:RDD[String] = sc.textFile("data/log.log")
    val per:RDD[(Int, String, String, String, Int)] = lines.map(line => {
      try {
        val arr:Array[String] = line.split(",")
        (arr(0).toInt, arr(1), arr(2), arr(3), arr(4).toInt)
      } catch {
        case e:Exception => null
      }
    })
    val filterPer:RDD[(Int, String, String, String, Int)] = per.filter(_ != null)
    val arr:Array[String] = filterPer.map(e => e._4).collect().distinct

    val rdd5:RDD[(String, Iterable[(Int, String, String, String, Int)])] = filterPer.groupBy(_._4, 3)
    val res1:RDD[(String, (Int, String, String, String, Int), String)] = rdd5.mapPartitionsWithIndex((p, iters) => {
      iters.flatMap(e => {
        e._2.map(e1 => {
          (e._1, e1, "分区为" + p)
        })
      })
    })
    res1.foreach(println)


    println("****************************************************************8")

    /**
     * 自定义分区
     */
    val rdd4:RDD[(String, Iterable[(Int, String, String, String, Int)])] = filterPer.groupBy(e => e._4, new MyPartitioner1(arr))

    val res:RDD[(String, (Int, String, String, String, Int), String)] = rdd4.mapPartitionsWithIndex((p, iters) => {
      iters.flatMap(e => {
        e._2.map(e1 => {
          (e._1, e1, "分区为" + p)
        })
      })
    })

    res.foreach(println)
  }

  private def MyPartitioner(sc:SparkContext):Unit = {
    val lines:RDD[String] = sc.textFile("data/score.csv")
    val stus:RDD[(Int, Student)] = lines.map(line => {
      val arr:Array[String] = line.split(",")
      (arr(0).toInt,Student(arr(0).toInt, arr(1), arr(2), arr(3).toDouble))
    })
    val rdd1:RDD[(Int, Iterable[Student])] = stus.groupByKey(new MyPartitions())
    val rdd2:RDD[(Int, Student, String)] = rdd1.mapPartitionsWithIndex((p, iter) => {
      iter.flatMap(e => {
        e._2.map(e1 => {
          (e._1, e1, "分区是" + p)
        })
      })
    })

    rdd2.foreach(println)
  }

  private def groupByKeyOperator(sc:SparkContext):Unit = {
    /**
     * map方法不会改变分区内的数据分布
     * groupBYKey算子产生了shuffle
     */
    val lines:RDD[String] = sc.textFile("data/score.csv", 2)
    val students:RDD[Student] = lines.map(line => {
      val arr:Array[String] = line.split(",")
      Student(arr(0).toInt, arr(1), arr(2), arr(3).toDouble)
    })
    val rdd:RDD[(Int, Student, String)] = students.mapPartitionsWithIndex((p, iters) => {
      iters.map(stu => {
        (stu.id, stu, "分区为" + p)
      })
    })
    rdd.foreach(println)

    println("******************************************")
    val rdd4:RDD[(Int, Iterable[Student])] = students.groupBy(stu => stu.id)
    val rdd5:RDD[(Int, Student, String)] = rdd4.mapPartitionsWithIndex((p, iters) => {
      iters.flatMap(tp => {
        tp._2.map(stu => {
          (tp._1, stu, "分区是" + p)
        })
      })
    })

    rdd5.foreach(println)
  }

  private def distinctOperator(rdd1:RDD[Int]):Unit = {
    /**
     * distinct 默认分区数不变,若数据量大大减少,可以手动减少分区数 coalesce
     */
    val res:RDD[Int] = rdd1.distinct()
    val res1:RDD[Int] = res.coalesce(2)
    println(res.getNumPartitions)
    println(res1.getNumPartitions)
  }

  private def intersectOperator(rdd1:RDD[Int], rdd2:RDD[Int]):Unit = {
    /**
     * intersection
     * 交集结果去重;最终分区结果为大的分区数
     */
    val res:RDD[Int] = rdd1.intersection(rdd2)
    println(res.getNumPartitions)
    res.collect().foreach(println)
  }

  private def unionOperator(rdd1:RDD[Int], rdd2:RDD[Int]):Unit = {
    /**
     * union:处理的数据类型要一致,分区个数可以不一致,最终分区数为两者和,结果不去重
     */
    val res:RDD[Int] = rdd1.union(rdd2)
    println(res.getNumPartitions)
    println("******************************************")
    res.foreach(println)
  }
}
