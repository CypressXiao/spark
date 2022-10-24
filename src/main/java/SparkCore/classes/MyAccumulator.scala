package SparkCore.classes

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @projectName: spark 
 * @package: SparkCore.classes
 * @className: MyAccumulator
 * @author: Cypress_Xiao
 * @description: 自定义累加器
 * @date: 2022/7/4 16:57
 * @version: 1.0
 */

//统计单词数,用可变map存储结果,[传入数据类型,输出数据类型]
class MyAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
  //定义返回结果的map
  private var mp = new mutable.HashMap[String, Int]()

  //什么情况下是0的方法
  override def isZero:Boolean = mp.isEmpty

  //创建新比较器的方法
  override def copy():AccumulatorV2[String, mutable.HashMap[String, Int]] = new MyAccumulator()

  //将返回数据集合清空的操作
  override def reset():Unit = mp.clear()

  //分区内操作记和的方法
  override def add(v:String):Unit = {
    var cnt:Int = mp.getOrElse(v, 0)
    cnt = cnt + 1;
    mp.put(v, cnt)
  }

  //分区间操作的方法
  override def merge(other:AccumulatorV2[String, mutable.HashMap[String, Int]]):Unit = {
    //其他分区累加器数据
    var mp2:mutable.HashMap[String, Int] = other.value
    //要输出的那个分区器数据集合
    var mp3:mutable.HashMap[String, Int] = this.value
    mp2.map(tp => {
      val word:String = tp._1
      var cnt1:Int = tp._2
      var cnt2:Int = mp3.getOrElse(word, 0)
      cnt2 += cnt1
      mp3.put(word, cnt2)
    })
  }

  override def value:mutable.HashMap[String, Int] = mp
}
