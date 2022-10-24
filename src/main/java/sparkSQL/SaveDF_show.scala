package sparkSQL

import java.util.Properties

import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: SaveDF_show
 * @author: Cypress_Xiao
 * @description: TODO  
 * @date: 2022/7/13 9:22
 * @version: 1.0
 */
object SaveDF_show {
  def main(args:Array[String]):Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sess:SparkSession = SparkSession.builder()
      .appName("show")
      .master("local[1]")
      .enableHiveSupport()
      .getOrCreate()
    Spark_Hive(sess)

    sess.close()
  }

  private def Spark_Hive(sess:SparkSession):Unit = {
    //使用格式优化之后需要注意关键字要``不然会出问题
    val df:DataFrame = sess.sql(
      """
        |SELECT
        |	`name`,
        |	dt1,
        |	max(cnt) AS max_cnt
        |FROM
        |	(
        |		SELECT
        |			`name`,
        |			dt1,
        |			count(1) AS cnt
        |		FROM
        |			(
        |				SELECT
        |					`name`,
        |					date_format(dt, "yyyy-MM") AS dt1,
        |					date_sub(dt, rn) AS dt2
        |				FROM
        |					(
        |						SELECT
        |							`name`,
        |							date(buy_time) AS dt,
        |							row_number () over (
        |								PARTITION BY `name`,
        |								date_format(buy_time, "yyyy-MM")
        |							ORDER BY
        |								buy_time
        |							) AS rn
        |						FROM
        |							doit32.users
        |					) AS t1
        |			) AS t2
        |		GROUP BY
        |			`name`,
        |			dt1,
        |			dt2
        |	) AS t3
        |GROUP BY
        |	`name`,
        |	dt1
        |""".stripMargin)

    println(df.rdd.partitions.length)
    df.explain("extended")


    //df.write.mode(SaveMode.Append).saveAsTable("doit32.users_res")
    //dataFrame也可以调用coalesce指定分区数
    //df.coalesce(1).write.format("orc").mode(SaveMode.Append).saveAsTable("doit32.users_res2")
  }

  private def toMySQL(sess:SparkSession):Unit = {
    val struct:StructType = new StructType()
      .add("id", DataTypes.LongType)
      .add("name", DataTypes.StringType)
      .add("sub", DataTypes.StringType)
      .add("score", DataTypes.DoubleType)
    val df:DataFrame = sess.read.schema(struct).csv("data/score.csv")
    val pro = new Properties()
    pro.setProperty("user", "root")
    pro.setProperty("password", "123456")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/my_db1", "tb_sparkScore", pro)
  }

  private def show(df:DataFrame):Unit = {
    df.show();
    df.show(2)
    df.show(false)
    df.show(2, false)
  }
}
