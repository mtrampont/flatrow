package row

import java.text.NumberFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._ // scalastyle:ignore underscore.import
import cats.macros.Ops

import scala.annotation.implicitNotFound
import scala.collection.mutable.{Buffer => MutBuffer}
import scala.language.experimental.macros

/**
  * Typeclass for types that we can flatten. The purpose is not to denormalize
  * but to reduce the level of nesting to none.
  *
  * @tparam T the data type we want flatten
  */
@implicitNotFound("Missing implicit flattener for ${T}")
trait FlatRow[T] {

  def flat(t: T): Vector[String]

  def row(t: T): Row

  def schema: StructType
}

trait FlatRowPrimitives {

  implicit final val FlatRowInt = new FlatRow[Int] {
    override def flat(i: Int): Vector[String] = Vector(NumberFormat.getIntegerInstance.format(i))

    override def row(i: Int): Row = Row(i)

    override def schema: StructType = StructType(Seq(StructField("", IntegerType, nullable = false)))
  }

  implicit final val FlatRowDouble = new FlatRow[Double] {
    override def flat(d: Double): Vector[String] = Vector(NumberFormat.getNumberInstance.format(d))

    override def row(d: Double): Row = Row(d)

    override def schema: StructType = StructType(Seq(StructField("", DecimalType.SYSTEM_DEFAULT, nullable = false)))
  }

  implicit final val FlatRowString = new FlatRow[String] {
    override def flat(d: String): Vector[String] = Vector(d)

    override def row(d: String): Row = Row(d)

    override def schema: StructType = StructType(Seq(StructField("", StringType, nullable = false)))
  }
}

object FlatRow extends FlatRowPrimitives {

  final val FieldNameSeparator: String = "_"

  final val EmptyValue: String = ""
  final val FieldValueSeparator: String = "-"

  def apply[T](implicit ev: FlatRow[T]): FlatRow[T] = ev

  implicit class FlatRowOps[A](a: A)(implicit flatA: FlatRow[A]) {
    def schema: StructType = flatA.schema

    def flat: Vector[String] = macro Ops.unop0[String]

    def row: Row = macro Ops.unop0[Row]
  }

  private[row] def prependFieldPrefix(prefix: String)(field: StructField): StructField = {
    val prefixedName =
      prefix +
        (if (field.name.nonEmpty) FieldNameSeparator else "") +
        field.name
    field.copy(name = prefixedName)
  }

  implicit def flatTuple2[A, B](implicit flatA: FlatRow[A], flatB: FlatRow[B]): FlatRow[(A,B)] = new FlatRow[(A, B)] {
    override def schema: StructType = StructType(
      flatA.schema.fields.map(prependFieldPrefix("1")) ++
      flatB.schema.fields.map(prependFieldPrefix("2"))
    )

    override def flat(t: (A, B)): Vector[String] = {
      val (a, b) = t
      flatA.flat(a) ++ flatB.flat(b)
    }

    override def row(t: (A, B)): Row = {
      val (a, b) = t
      Row.merge(flatA.row(a), flatB.row(b))
    }
  }

  implicit def flatCollection[A, Col[X] <: Iterable[X]](implicit flatA: FlatRow[A]): FlatRow[Col[A]] =
    new FlatRow[Col[A]] {
      override def schema: StructType = {
        val fields =
          flatA.schema.fields
          .zipWithIndex
          .map{ case (field, idx) =>
            StructField(
              name = field.name,
              dataType = ArrayType(field.dataType, field.nullable),
              nullable = false
            )
          }
        StructType(fields)
      }

      override def flat(t: Col[A]): Vector[String] = {
        t.foldLeft(Vector.fill(flatA.schema.fields.length)(MutBuffer.empty[String])){ case (accum, a) =>
          accum.zip(flatA.flat(a))
            .foreach{ case (accumField, field) =>
              accumField += field
            }
          accum
        }.map(_.mkString(FieldValueSeparator))
      }

      override def row(t: Col[A]): Row = {
        val accumFields = Seq.fill(flatA.schema.fields.length)(MutBuffer.empty[Any])

        val fields =
          t.foldLeft(accumFields){ case (accum, a) =>
            (accum, flatA.row(a).toSeq).zipped.foreach { case (accumField, field) =>
              accumField.+=(field)
            }
            accum
          }.map(_.toSeq)
        Row(fields: _*)
      }
    }

  implicit def flatOption[A](implicit flatA: FlatRow[A]): FlatRow[Option[A]] =
    new FlatRow[Option[A]] {
      override def schema: StructType = {
        val s = flatA.schema
        s.copy(fields = s.fields.map(_.copy(nullable = true)))
      }

      override def flat(t: Option[A]): Vector[String] = t match {
        case Some(a) => flatA.flat(a)
        case None => Vector.fill(flatA.schema.fields.length)(EmptyValue)
      }

      override def row(t: Option[A]): Row = t match {
        case Some(a) => flatA.row(a)
        case None => Row(Seq.fill(flatA.schema.fields.length)(null): _*) // scalastyle:ignore null
      }
    }

}
