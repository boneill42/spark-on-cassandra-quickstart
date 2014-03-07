package com.github.boneill42

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import com.tuplejump.calliope.utils.RichByteBuffer
import org.apache.spark.SparkContext

import com.tuplejump.calliope.Implicits._
import com.tuplejump.calliope.CasBuilder
import com.tuplejump.calliope.Types.{CQLRowMap, CQLRowKeyMap, ThriftRowMap, ThriftRowKey}
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class FindChildrenTest extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  import Cql3CRDDTransformers._

  val sc = new SparkContext("local[1]", "castest")

  describe("Find All Children in IRL") {
    it("should be able find children in IRL") {

      val cas = CasBuilder.cql3.withColumnFamily("northpole", "children")

      val cqlRdd = sc.cql3Cassandra[Child](cas)

      val children = cqlRdd.collect().toList
      children.filter((child) => child.country.equals("IRL")).map((child) => println(child))
      sc.stop()
    }

  }

  override def afterAll() {
    sc.stop()
  }
}

private object Cql3CRDDTransformers {

  import RichByteBuffer._

  implicit def row2String(key: ThriftRowKey, row: ThriftRowMap): List[String] = {
    row.keys.toList
  }

  implicit def cql3Row2Child(keys: CQLRowKeyMap, values: CQLRowMap): Child = {
    Child(keys.get("child_id").get, values.get("first_name").get, values.get("last_name").get, values.get("country").get, values.get("state").get, values.get("zip").get)

  }
}

case class Child(childId: String, firstName: String, lastName: String, country: String, state: String, zip: String)
