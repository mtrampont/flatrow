package row

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._



class FlatMacrosTest extends common.UnitTest {

  import FlatRow._

  case class SampleData(
    someInt: Int,
    someDouble: Double,
    optionalString: Option[String],
    tuple: (Int, String)
  )

  val flatSampleData = new FlatRow[SampleData] {
    override def flat(t: SampleData): Vector[String] = {
      Vector.empty[String] ++
        FlatRow[Int].flat(t.someInt) ++
        FlatRow[Double].flat(t.someDouble) ++
        FlatRow[Option[String]].flat(t.optionalString) ++
        FlatRow[(Int, String)].flat(t.tuple)
    }

    override def row(t: SampleData): Row = {
      Row.merge(
        FlatRow[Int].row(t.someInt),
        FlatRow[Double].row(t.someDouble),
        FlatRow[Option[String]].row(t.optionalString),
        FlatRow[(Int, String)].row(t.tuple)
      )
    }

    override def schema: StructType = {
      StructType(
        FlatRow[Int].schema.fields.map(prependFieldPrefix("someInt")) ++
          FlatRow[Double].schema.fields.map(prependFieldPrefix("someDouble")) ++
          FlatRow[Option[String]].schema.fields.map(prependFieldPrefix("optionalString")) ++
          FlatRow[(Int, String)].schema.fields.map(prependFieldPrefix("tuple"))
      )
    }
  }

  s"${FlatMacros.getClass.getName}" should "provide FlatRow instance for non-nested case class" in {
    import FlatMacros._

    val autoFlatSample = implicitly[FlatRow[SampleData]]

    val sample =
      SampleData(
        someInt = 1,
        someDouble = 10.0,
        optionalString = None,
        tuple = (9, "foo")
      )

    autoFlatSample.flat(sample) should ===(flatSampleData.flat(sample))
    autoFlatSample.flat(sample) should ===(Vector("1", "10", "", "9", "foo"))
    autoFlatSample.row(sample).toSeq should ===(flatSampleData.row(sample).toSeq)
    autoFlatSample.schema should ===(flatSampleData.schema)

  }

  it should "provide FlatRow instance for nested case class" in {
    import FlatMacros._

    case class NestedSampleData(
      someString: String,
      someSample: SampleData
    )

    val autoFlatNestedSample = implicitly[FlatRow[NestedSampleData]]

    val nestedSample =
      NestedSampleData(
        someString = "bar",
        SampleData(
          someInt = 1,
          someDouble = 10.0,
          optionalString = None,
          tuple = (9, "foo")
        )
      )

    autoFlatNestedSample.flat(nestedSample) should ===(Vector("bar", "1", "10", "", "9", "foo"))
    autoFlatNestedSample.row(nestedSample).toSeq should ===(Row("bar", 1, 10.0, null, 9, "foo").toSeq)
    autoFlatNestedSample.schema.fields should ===(
      Array(
        StructField(name = "someString", dataType = StringType, nullable = false),
        StructField(name = "someSample_someInt", dataType = IntegerType, nullable = false),
        StructField(name = "someSample_someDouble", dataType = DecimalType.SYSTEM_DEFAULT, nullable = false),
        StructField(name = "someSample_optionalString", dataType = StringType, nullable = true),
        StructField(name = "someSample_tuple_1", dataType = IntegerType, nullable = false),
        StructField(name = "someSample_tuple_2", dataType = StringType, nullable = false)
      )
    )

  }

  it should "produce an intelligible error at compile time in case a field type is missing a type class" in {
    import FlatMacros._

    case class NotFlattableSampleData(
      someString: String,
      someBool: Boolean
    )

    "val autoFlatSample = implicitly[FlatRow[NotFlattableSampleData]]" shouldNot typeCheck
  }

  it should "still be possible to customize serialization of the case class" in {

    val sample = CustomFlatData(
      someString = "foo",
      someAmount = 0.6666
    )

    sample.flat should ===(Vector("foo", "0,67"))
    sample.row.toSeq should ===(Row("foo", 0.6666).toSeq)
    sample.schema.fields should ===(
      Array(
        StructField(name = "someString", dataType = StringType, nullable = false),
        StructField(name = "someAmount", dataType = DecimalType(DecimalType.MAX_PRECISION, 2), nullable = false)
      )
    )
  }

}
