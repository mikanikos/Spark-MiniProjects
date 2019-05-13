package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

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

    val typeName = rdd.first().schema.fields(indexAgg).dataType.typeName

    // Casts numeric data types into their respective type for aggregation
    val cast = (x: Any) => typeName match {
      case "integer" => x.asInstanceOf[Int] // Infers further cast Int->Double
      case "double" => x.asInstanceOf[Double]
      case _ => Double.NaN
    }

    val aggf = (ds: (Double, Double)) =>
    {
      agg match {
        case "COUNT" => ds._2
        case "AVG" => ds._1/ds._2
        case _ =>  ds._1
      }
    }

    // Helper function to construct the name of the result
    val makeName = {
      (ks: Map[Int,Any]) => index.map(i=>if(ks.contains(i)) ks(i).toString else '*').mkString(",")
    }

    // Create lattice space C mentioned in literature
    val lattice = index.toSet.subsets().map(x=>x.toList).toList

    val getKey = (r: Row) => index.map(i => (i, r(i))).toMap

    // Maps Iterator[Row] => Iterator[(k, v)] where k is grouping values and v is (sum, count)
    val mapPhase = (x: Iterator[Row]) => x.map(r => (getKey(r), (cast(r(indexAgg)), 1.0)))
    // Combines within partition: Iterator[(k, v)] => Iterator[(k, v)] with like keys combined
    val combine = (t: Iterator[(Map[Int, Any], (Double ,Double))]) => {
      val m = scala.collection.mutable.HashMap[Map[Int,Any], (Double, Double)]().withDefaultValue((0.0,0.0))
      t.foreach(r => {
        val ds = m(r._1)
        val rval = r._2
        m(r._1) = (ds._1+rval._1,ds._2+rval._2)
      })
      m.iterator
    }
    // Encapsulate map and mapper-side combine into single mapper function
    val mapCombine = (x: Iterator[Row]) => combine(mapPhase(x))

    // Reduce function - transforms a (k, [values]) pair into (k, M[values]) by pairwise operation on values
    val reducePhase = {
      (ds1 : (Double, Double), ds2: (Double, Double)) => (ds1._1+ds2._1, ds1._2+ds2._2)
    }

    // Expands the from a bottom lattice (k, v) pair the partial results above
    val expandLattice = (t: (Map[Int,Any], (Double, Double))) => {
      lattice.map(lx => {
        (t._1.filterKeys(lx.contains).toArray.toMap, t._2)
      })
    }

    rdd.mapPartitions(mapCombine) // Map phase
      .reduceByKey(reducePhase) // Reduce phase
      .flatMap(expandLattice) // Expand lattice
      .mapPartitions(combine) // Expanded map/combine
      .reduceByKey(reducePhase) // Expanded reduction
      .map(x => (makeName(x._1), aggf(x._2)))
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
    val aggf = (ds: (Double, Double)) =>
    {
      agg match {
        case "COUNT" => ds._2
        case "AVG" => ds._1/ds._2
        case _ =>  ds._1
      }
    }

    // Helper function to construct the name of the result
    val makeName = {
      (ks: Map[Int,Any]) => index.map(i=>if(ks.contains(i)) ks(i).toString else '*').mkString(",")
    }

    // Map function - transform tuple t into (k,t(i)) pairs, where k is key, t(i) is aggregation column
    val mapPhase = {
      (t: Row) => lattice.map(x => (x.map(i => (i,t(i))).toMap, ((cast(t(indexAgg))), 1.0)))
    }

    // Reduce function - transforms a (k, [values]) pair into (k, M([values])) by pairwise operation on values
    val reducePhase = (d1: (Double, Double), d2: (Double, Double)) => {
      agg match {
        case "MIN" => (Math.min(d1._1, d2._1), 0.0)
        case "MAX" => (Math.max(d1._1, d2._1), 0.0)
        case _ => (d1._1+d2._1, d1._2+d2._2)
      }
    }

    // Map -> Sort/Shuffle -> Reduce -> Format Output
    rdd.flatMap(mapPhase)
      .reduceByKey(reducePhase)
      .map(x => (makeName(x._1), aggf(x._2)))
  }
}
