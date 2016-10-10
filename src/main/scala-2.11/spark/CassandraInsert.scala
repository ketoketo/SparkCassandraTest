package spark

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.cassandra._


/**
  * Created by featm on 2016/10/08.
  */
object CassandraInsert {

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf(true)
//      .set("spark.cassandra.connection.host", "127.0.0.1")
//      .setMaster("local[*]")
//      .setAppName(getClass.getName)
//      .set("cassandra.username", "cassandra")
//      .set("cassandra.password", "cassandra")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("My App")
      .config("spark.sql.warehouse.dir", "file:///C:/path/to/my/")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .config("cassandra.username", "cassandra")
      .config("cassandra.password", "cassandra")
      .getOrCreate()

//    spark.read.cassandraFormat("users", "summarydb").load().createOrReplaceTempView("users")
//    spark.sql("SELECT count(*) from users").show()

    // 一度データを読み込んで再度インサート
//    spark.read.cassandraFormat("users", "summarydb").load().createOrReplaceTempView("users_tmp")
//    val data = spark.sql("SELECT * FROM users_tmp limit 100").rdd.map(row => (row.getString(1), row.getString(0)))
//    data.take(1)
//    data.saveToCassandra("summarydb", "users", SomeColumns("userid", "first_name"))
//      .saveToCassandra("summarydb", "users", SomeColumns("userid", "first_name"))

//    val test = spark.createDataset(Seq(("make", "BBB"),("12", "CCC"))).rdd
//    test.take(1)
//      test.saveToCassandra("summarydb", "users", SomeColumns("userid", "first_name"))


//    val sc = new SparkContext(conf)

    // create tableから
    //    val rdd = sc.parallelize(1 to 3000000).map(t => (t.toString, "AAAA")).saveAsCassandraTable("summarydb", "users", SomeColumns("userid", "first_name"))

    // insert
    //    val rdd = sc.parallelize(1 to 3000000).map(t => (t.toString, "AAAA")).saveToCassandra("summarydb", "users", SomeColumns("userid", "first_name"))

    // select
//        var rdd = sc.cassandraTable("summarydb", "users")
//        println(rdd.count())

    // 2.0.0のセレクト
//    val sqlContext = new SQLContext(sc)
//    sqlContext.read.cassandraFormat("users", "summarydb").load().createOrReplaceTempView("users_tmp")
//    val rdd = sqlContext.sql("SELECT userid FROM users_tmp")
//    rdd.collect().foreach(println)


    // select(CassandraContext) 2.0.0で廃止された
    //    val cc = new CassandraSQLContext(sc)
    //    cc.setKeyspace("summarydb")
    //    cc.cassandraSql("select userid, first_name from users").show
  }

}
