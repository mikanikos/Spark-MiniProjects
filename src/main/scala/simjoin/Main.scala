package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.io._

object Main {
  def main(args: Array[String]) {     
    val inputFile="../dblp_1K.csv"
    val numAnchors = 4
    val distanceThreshold = 2
    val attrIndex = 0    
        
    val input = new File(getClass.getResource(inputFile).getFile).getPath
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(input)
    
    val rdd = df.rdd        
    val schema = df.schema.toList.map(x => x.name)    
    val dataset = new Dataset(rdd, schema)


    val t1 = System.nanoTime    
    val sj = new SimilarityJoin(numAnchors, distanceThreshold)
    val res = sj.similarity_join(dataset, attrIndex)           
    
    val resultSize = res.count
    val t2 = System.nanoTime

    // cartesian
    val t1Cartesian = System.nanoTime
    val cartesian = rdd.map(x => (x(attrIndex), x)).cartesian(rdd.map(x => (x(attrIndex), x)))
                                   .filter(x => (x._1._2(attrIndex).toString() != x._2._2(attrIndex).toString() && distance(x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString()) <= distanceThreshold))

    //println(cartesian.count)
    val cartesianSize = cartesian.count()
    val t2Cartesian = System.nanoTime


    val cartesian_res = cartesian.map(x => ((x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString)))

    val diff = cartesian_res.distinct().subtract(res)

    println(resultSize, (t2-t1)/(Math.pow(10,9)), cartesianSize, (t2Cartesian-t1Cartesian)/(Math.pow(10,9)), diff.count())

  }

  // from here https://rosettacode.org/wiki/Levenshtein_distance#Scala
  def distance(s1: String, s2: String): Int = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

    @inline
    def minimum(i: Int*): Int = i.min

    for {j <- dist.indices.tail
         i <- dist(0).indices.tail} dist(j)(i) =
      if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)

    dist(s2.length)(s1.length)
  }

}
