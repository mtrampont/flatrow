package row

import common.UnitTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


class FlatRowTest extends UnitTest {

  import FlatRow.FlatRowOps

  behavior of "FlatRow"

  it should "flatten primitives" in {
    val double = 2.0
    val int = 1
    val string = "foo"


    double.schema should ===(StructType(Seq(StructField(name = "", DecimalType.SYSTEM_DEFAULT, nullable = false))))
    int.schema should ===(StructType(Seq(StructField(name = "", IntegerType, nullable = false))))
    string.schema should ===(StructType(Seq(StructField(name = "", StringType, nullable = false))))

    double.flat should ===(Vector("2"))
    int.flat should ===(Vector("1"))
    string.flat should ===(Vector(string))

    double.row should ===(Row(2.0))
    int.row should ===(Row(1))
    string.row should ===(Row(string))

  }

  it should "flatten tuples" in {
    val tuple = ("foo", 1)

    tuple.schema should ===(StructType(
      Seq(
        StructField(name = "1", StringType, nullable = false),
        StructField(name = "2", IntegerType, nullable = false)
      )
    ))
    tuple.flat should ===(Vector("foo", "1"))
    tuple.row should ===(Row("foo", 1))

  }

  it should "flatten collections" in {
    val list = List(1, 3, 5)
    val vector = Vector(2, 4, 6, 8)

    list.schema should ===(
      StructType(Seq(StructField(name = "", ArrayType(IntegerType, containsNull = false), nullable = false)))
    )
    list.flat should ===(Vector("1-3-5"))
    val lrow = list.row
    lrow should have length 1
    lrow.getList[Int](0) should contain theSameElementsInOrderAs list

    vector.schema should ===(
      StructType(Seq(StructField(name = "", ArrayType(IntegerType, containsNull = false), nullable = false)))
    )
    vector.flat should ===(Vector("2-4-6-8"))
    val vrow = vector.row
    vrow should have length 1
    vrow.getSeq[Int](0) should contain theSameElementsInOrderAs vector

  }

  it should "flatten options" in {
    val someOption = Option(2.0)
    val emptyOption: Option[Double] = None

    someOption.schema should ===(StructType(Seq(StructField(name = "", DecimalType.SYSTEM_DEFAULT, nullable = true))))
    someOption.flat should ===(Vector("2"))
    someOption.row should ===(Row(2.0))

    emptyOption.schema should ===(StructType(Seq(StructField(name = "", DecimalType.SYSTEM_DEFAULT, nullable = true))))
    emptyOption.flat should ===(Vector(""))
    emptyOption.row should ===(Row(null))
  }

}
