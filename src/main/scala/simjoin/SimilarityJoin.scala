package simjoin

import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.{SparkContext, sql}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = null
  
  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */  
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {

    // Get rdd and take the row we want to apply the similarity join
    val rdd = dataset.getRDD()
    val row_rdd = rdd.map(x => x.getAs[String](attrIndex))

    // Get size of the dataset and compute probability pA for sampling anchor points
    val prob_A = (numAnchors.toDouble / (row_rdd.count().toDouble))

    // Sample anchors with given probability
    val anchors = row_rdd.sample(false, prob_A)

    // Insert index on the anchor points
    val anchors_index = anchors.zipWithIndex()

    // Cartesian product between the row and the anchors in order to compare each value with each anchor
    val cartesian_prod = row_rdd.cartesian(anchors_index)

    // Compute edit distance
    val cartesian_dist = cartesian_prod.map(x => (x._1, (x._2._2, distance(x._1, x._2._1))))

    // Assign to each row an home partition by selecting the minimum distance among all the anchors
    val home_partitions = cartesian_dist.reduceByKey((a,b) => if (a._2 > b._2) b else a)

    // Assign to each row all the outer partitions that satisfy the condition
    val outer_partitions = home_partitions.join(cartesian_dist).filter(x => x._2._2._2 <= x._2._1._2 + 2 * distThreshold).map(x => (x._2._2._1, x._1))

    // Join outer partitions with home partitions respectively to each anchor in order to compare them
    val similair_pairs = home_partitions.map(x => (x._2._1, x._1)).join(outer_partitions).map(x => x._2)

    // Filter each pair according to their distance and don't consider equal strings
    val result = similair_pairs.filter(x => x._1 != x._2 && distance(x._1, x._2) <= distThreshold)

    // Take also the swapped pairs and remove duplicates
    val result_clean = result.flatMap(x => List(x, x.swap)).distinct()

    result_clean

  }



  // algorithm taken from here https://rosettacode.org/wiki/Levenshtein_distance#Scala
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

