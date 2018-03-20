package row
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType



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

  "FlatMacros" should "provide FlatRow instance for non-nested case class" in {
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
    autoFlatSample.row(sample).toSeq should ===(flatSampleData.row(sample).toSeq)
    autoFlatSample.schema should ===(flatSampleData.schema)

  }


}
