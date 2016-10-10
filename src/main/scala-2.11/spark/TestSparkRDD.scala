package spark

import org.apache.spark.sql.SparkSession

/**
  * Created by featm on 2016/10/11.
  */
object TestSparkRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("My App")
      .getOrCreate()
    val rdd = spark.sparkContext.parallelize(Seq(
      ("AAA", new Exam("数学", 100)),
      ("AAA", new Exam("英語", 70)),
      ("AAA", new Exam("国語", 60))))

    val reduceRDD = rdd.reduceByKey((a, b) => {
      new Exam(a.kamoku, a.point + b.point)
    })
    reduceRDD.foreach(f => println(f._2.point))

  }
}

case class Exam(kamoku: String, point: Int)
