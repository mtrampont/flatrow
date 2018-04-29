package row

import java.math.RoundingMode
import java.text.NumberFormat
import java.util.Locale

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DecimalType, StructField, StructType}

case class CustomFlatData(
  someString: String,
  someAmount: Double
)
object CustomFlatData {
  implicit val flatAmount: FlatRow[Double] = new FlatRow[Double] {
    private val amountFormatter = NumberFormat.getNumberInstance(Locale.FRENCH)
    amountFormatter.setMaximumFractionDigits(2)
    amountFormatter.setRoundingMode(RoundingMode.HALF_UP)

    override def flat(t: Double): Vector[String] = Vector(amountFormatter.format(t))

    override def row(d: Double): Row = Row(d)

    override def schema: StructType =
      StructType(Seq(StructField("", DecimalType(DecimalType.MAX_PRECISION, 2), nullable = false)))
  }

  implicit val flatRow: FlatRow[CustomFlatData] = FlatMacros.flatRowCaseClass[CustomFlatData]
}