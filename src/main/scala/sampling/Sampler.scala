package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.commons.math3.special.Erf

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Sampler {

  val aggCol = 0 // Index of column we calculate variance from
  val QCSs = List(List(1), List(1)) // Lists of QCS, ranked most to least beneficial

  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    val rdd = lineitem.rdd

    val z = Math.sqrt(2)*Erf.erfInv(ci) // z_(1-alpha/2) = sqrt(2) * erf(alpha)

    // Pay the price to gather statistics for error calculations
    val columnCount = rdd.count()
    val columnSum = rdd.map(r=>r(aggCol).asInstanceOf[Double]).sum()
    // Construct results we pass: [RDD] of samples and Map[QCS: index of sample, scale factor]
    val samples = new ListBuffer[RDD[(Row, Double)]]
    val indices = new ListBuffer[(List[Int], Int)]
    var budgetUsed = 0:Long

    // For every QCS we manually created, compute its cost and add it if in budget
    // Manually created QCS list is ordered by utility of QCS in our examinations
    QCSs.foreach( QCS => {
      if(budgetUsed < storageBudgetBytes) {
        val keyed = rdd.map(r => (QCS.map(i => r(i)), r))
        val stratified = keyed.groupByKey()
        var satisfiesConstraint = false
        var k = Math.sqrt(columnCount/2).toInt

        while (!satisfiesConstraint) {
          val stratSample = sampleK(stratified, k, 0.000005)
          val error = (z * Math.sqrt(stratSample._2)) / columnSum // Calculate error within ci
          satisfiesConstraint = error < e
          if (satisfiesConstraint) {
            val sampleBudget = stratSample._1.count()
            if(budgetUsed+sampleBudget<storageBudgetBytes) {
              budgetUsed += sampleBudget
              samples.append(stratSample._1)
              indices.append((QCS, samples.length))
            }
          } else {
            k += (k*(error/e)).toInt
          }
        }
      }
    })
    // Return list of RDDS, plus a Map of (QCS : Map(QCS:Index)) pairs
    // Each RDD entry is of type (Row, Double), where the double represents the 'scaling' of the sample
    (samples.toList, indices.toMap)
  }

  private def sampleK(rdd: RDD[(List[Any], Iterable[Row])], k: Int, delta: Double): (RDD[(Row, Double)], Double) = {
    val a = rdd.mapValues(x=>srs(x, k, delta))
    val b = a.values
    (b.keys.flatMap(identity), b.values.sum)
  }

  private def srs(values: Iterable[Row], k: Int, delta: Double): (Iterable[(Row, Double)], Double) = {
    // Calculate thresholds for accept/reject
    val n = values.size
    val p = k.toDouble/n
    val g1 = -Math.log(delta)/n
    val g2 = (-2*Math.log(delta))/(3*n)
    val q1 = Math.min(1.0, p+g1+Math.sqrt(g1*g1+2*g1*p))
    val q2 = Math.max(0.0, p+g2-Math.sqrt(g2*g2+3*g2*p))

    // Build buffers for accepted and wait-listed terms
    var accept = new ListBuffer[Row]
    var waitlist = new ListBuffer[(Double, Row)]
    var l = 0
    var m = 0.0
    var s = 0.0
    var count = 0
    values.foreach(v => {
      // Single pass to compute population variance
      val sampled = v(aggCol).asInstanceOf[Double]
      val prevm = m
      count += 1
      m = m + (sampled-m)/count
      s = s + (sampled-m)*(sampled-prevm)
      // Assign random value to tuple and accept/deny/wait-list it
      val key = Random.nextDouble()
      if (key < q2) {
        accept += v
        l += 1
      } else if (key < q1) {
        waitlist += Tuple2(key, v)
      }
    })
    // Take top k-l elements from the sorted wait list
    val elts = waitlist.sortBy(x=>x._1).take(k-l)
    accept.append(elts.map(x=>x._2):_*)
    // Compute variance and summand (use in computing errors)
    val variance = s/(n-1)
    val summand = (n.toDouble*n.toDouble*variance)/(accept.length)
    // Return sample and summand
    (accept.map(r=>(r,n.toDouble)).toList, summand)
  }

}
