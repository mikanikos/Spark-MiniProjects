package cubeoperator

import org.apache.spark.rdd.RDD

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    //TODO Task 1

    null
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    // Create lattice space as List[List[Int]] - each List[Int] is a configuration of inputs
    val lattice = index.toSet.subsets().map(x=>x.toList).toList

    // Map function - transform tuple t into (k,t) pair, where k is the grouping key
    // Function: Row => List[(List[Any], Row)]
    val mapPhase = {
      (t: org.apache.spark.sql.Row) => lattice.map(x => (x.map(i => t(i)), t))
    }
    // Reduce function - transform a (k, [values]) pair into aggregation
    // Function: (k, Row) => (String, Double)
    // TODO: Extend this for other aggregation operations and column types
    // TODO: Modify output String with * for 'all columns'
    val reducePhase = {
      (t: (List[Any], scala.Iterable[(List[Any], org.apache.spark.sql.Row)])) =>
      (t._1.mkString(","),t._2.map(x=>x._2(indexAgg).asInstanceOf[Int].toDouble).sum)
    }

    rdd.flatMap(mapPhase)
      .groupBy(k=>k._1)
      .map(reducePhase)
  }

}
