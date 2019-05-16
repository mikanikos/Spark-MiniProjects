package sampling

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

object Executor {

  def query_executor(session: SparkSession, params: List[Any], file: String): sql.DataFrame = {
    val path = "../queries/" + file
    val query_file = new File(getClass.getResource(path).getFile).getPath
    var query_string = scala.io.Source.fromFile(query_file).mkString
    for (p <- 0 to params.length-1) {
      query_string = query_string.replaceAll(":" + (p+1), params(p).toString())
    }
    session.sql(query_string)
  }

  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    query_executor(session, params, "1.sql")
  }

  def execute_Q3(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.orders.createOrReplaceTempView("orders")
    desc.customer.createOrReplaceTempView("customer")
    query_executor(session, params, "3.sql")
  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.orders.createOrReplaceTempView("orders")
    desc.customer.createOrReplaceTempView("customer")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    query_executor(session, params, "5.sql")
  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    query_executor(session, params, "6.sql")
  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.orders.createOrReplaceTempView("orders")
    desc.customer.createOrReplaceTempView("customer")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    query_executor(session, params, "7_view.sql").createOrReplaceTempView("shipping")
    query_executor(session, List(), "7.sql")
  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.orders.createOrReplaceTempView("orders")
    desc.part.createOrReplaceTempView("part")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.partsupp.createOrReplaceTempView("partsupp")
    query_executor(session, params, "9_view.sql").createOrReplaceTempView("profit")
    query_executor(session, List(), "9.sql")

  }

  def execute_Q10(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.orders.createOrReplaceTempView("orders")
    desc.customer.createOrReplaceTempView("customer")
    desc.nation.createOrReplaceTempView("nation")
    query_executor(session, params, "10.sql")
  }

  def execute_Q11(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.partsupp.createOrReplaceTempView("partsupp")
    query_executor(session, params, "11.sql")
  }

  def execute_Q12(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.orders.createOrReplaceTempView("orders")
    query_executor(session, params, "12.sql")
  }

  def execute_Q17(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    query_executor(session, params, "17.sql")
  }

  def execute_Q18(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.orders.createOrReplaceTempView("orders")
    desc.customer.createOrReplaceTempView("customer")
    query_executor(session, params, "18.sql")
  }

  def execute_Q19(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    query_executor(session, params, "19.sql")
  }

  def execute_Q20(desc: Description, session: SparkSession, params: List[Any]) = {
    desc.lineitem.createOrReplaceTempView("lineitem")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.partsupp.createOrReplaceTempView("partsupp")
    desc.part.createOrReplaceTempView("part")
    query_executor(session, params, "20.sql")
  }
}
