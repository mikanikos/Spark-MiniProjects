package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.SetQuantifierContext
import org.dmg.pmml.True

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
    // Extract column type from schema to cast as correct Numeric type
    val typeName = rdd.first().schema.fields(indexAgg).dataType.typeName

    // Create lattice space C mentioned in literature
    val lattice = index.toSet.subsets().map(x=>x.toList).toList

    // Casts numeric data types into their respective type for aggregation
    val cast = (x: Any) => typeName match {
      case "integer" => x.asInstanceOf[Int] // Infers further cast Int->Double
      case "double" => x.asInstanceOf[Double]
      case _ => Double.NaN
    }

    // Define measure function M referenced in literature
    // TODO: Compute average efficiently (one pass)
    val aggf = (ls: Iterable[Any]) =>
    {
      agg match {
        case "COUNT" => ls.size.asInstanceOf[Double]
        case "SUM" => ls.map(cast).sum
        case "MIN" => ls.map(cast).min
        case "MAX" => ls.map(cast).max
        case "AVG" => ls.map(cast).sum/ls.size.asInstanceOf[Double]
        case _ => throw new IllegalArgumentException(agg + " is not a valid aggregation function")
      }
    }

    // Helper function to construct the name of the result
    val makeName = {
      (ks: Map[Int,Any]) => index.map(i=>if(ks.contains(i)) ks(i).toString else '*').mkString(",")
    }

    // Map function - transform tuple t into (k,t(i)) pairs, where k is key, t(i) is aggregation column
    val mapPhase = {
      (t: Row) => lattice.map(x => (x.map(i => (i,t(i))).toMap, t(indexAgg)))
    }

    // Reduce function - transform a (k, [values]) pair into (k, M([values]))
    val reducePhase = {
      (t: (Map[Int, Any], Iterable[Any])) =>
      (makeName(t._1),aggf(t._2))
    }

    // Map -> Sort/Shuffle -> Reduce
    rdd.flatMap(mapPhase)
      .groupByKey()
      .map(reducePhase)
  }
}
