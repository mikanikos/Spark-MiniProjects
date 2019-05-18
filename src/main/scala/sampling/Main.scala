package sampling

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Main {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val session = SparkSession.builder().getOrCreate();

    val rdd = RandomRDDs.uniformRDD(sc, 100000)
    val rdd2 = rdd.map(f => Row.fromSeq(Seq(f * 2, (f*10))))

    val table = session.createDataFrame(rdd2, StructType(
      StructField("A1", DoubleType, false) ::
      StructField("A2", DoubleType, false) ::
      Nil
    ))

    val desc = new Description
    desc.lineitem = table
    desc.e = 0.1
    desc.ci = 0.95

//
//    val tmp = Sampler.sample(desc.lineitem, 1000000, desc.e, desc.ci)
//    desc.samples = tmp._1
//    desc.sampleDescription = tmp._2
//
//    // check storage usage for samples
//
//    // Execute first query
//    Executor.execute_Q1(desc, session, List("3"))

    val path = "src/main/resources/tpch_parquet_sf1/"

    desc.lineitem = session.read.parquet(path + "lineitem.parquet")
    desc.customer = session.read.parquet(path + "customer.parquet")
    desc.orders = session.read.parquet(path + "order.parquet")
    desc.supplier = session.read.parquet(path + "supplier.parquet")
    desc.nation = session.read.parquet(path + "nation.parquet")
    desc.region = session.read.parquet(path + "region.parquet")
    desc.part = session.read.parquet(path + "part.parquet")
    desc.partsupp = session.read.parquet(path + "partsupp.parquet")


    // SHOW TABLES
//    desc.lineitem.show()
//    desc.customer.show()
//    desc.orders.show()
//    desc.supplier.show()
//    desc.nation.show()
//    desc.region.show()
//    desc.part.show()
//    desc.partsupp.show()


    // EXECUTE QUERIES
//    println("1")
//    Executor.execute_Q1(desc, session, List("3")).show()
//    println("3")
//    Executor.execute_Q3(desc, session, List("AUTOMOBILE", "1996-12-01")).show()
//    println("5")
//    Executor.execute_Q5(desc, session, List("AMERICA", "1996-12-01")).show()
//    println("6")
//    Executor.execute_Q6(desc, session, List("1996-12-01", "0.09", "2")).show()
//    println("7")
//    Executor.execute_Q7(desc, session, List("CANADA", "CHINA")).show()
//    println("9")
//    Executor.execute_Q9(desc, session, List("brown")).show()
//    println("10")
//    Executor.execute_Q10(desc, session, List("1996-12-01")).show()
//    println("11")
//    Executor.execute_Q11(desc, session, List("ALGERIA", "2")).show()
//    println("12")
//    Executor.execute_Q12(desc, session, List("AIR", "TRUCK", "1996-12-01")).show()
//    println("17")
//    Executor.execute_Q17(desc, session, List("Brand#13", "LG CASE")).show()
//    println("18")
//    Executor.execute_Q18(desc, session, List("5")).show()
//    println("19")
//    Executor.execute_Q19(desc, session, List("Brand#13", "Brand#24", "Brand#11", "2", "3", "4")).show()
//    println("20")
//    Executor.execute_Q20(desc, session, List("green", "1996-12-01", "CANADA")).show()


  }     
}
