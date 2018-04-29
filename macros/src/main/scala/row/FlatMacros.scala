package row

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.{Context => BlackContext}

object FlatMacros {

  implicit def flatRowCaseClass[D <: Product]: FlatRow[D] = macro flatRowImpl[D]

  // Multiple string literals check does not work well with interpolated strings (e.g. quasiquotes)
  // scalastyle:off multiple.string.literals
  def flatRowImpl[D <: Product : c.WeakTypeTag](c: BlackContext): c.Expr[FlatRow[D]] = {
    import c.universe._ // scalastyle:ignore

    val dataType = implicitly[c.WeakTypeTag[D]]
    val members = dataType.tpe.members.sorted.filter(s => s.isMethod && s.asMethod.isCaseAccessor)

    if (members.isEmpty) {
      throw new UnsupportedOperationException(s"FlatRow generation not supported for empty class $dataType")
    }

    val membersFlats =
      members.map { mem =>
        val memberType = mem.typeSignature.resultType
        q"implicitly[_root_.row.FlatRow[$memberType]].flat(t.$mem)"
      }.fold(q"""Vector.empty[String]"""){ case (accumTree, flatMem) => q"$accumTree ++ $flatMem" }
    val flatQuote = q"override def flat(t: ${dataType.tpe}): Vector[String] = $membersFlats"

    val membersRows =
      members.map { mem =>
        val memberType = mem.typeSignature.resultType
        q"implicitly[_root_.row.FlatRow[$memberType]].row(t.$mem)"
      }
    val rowQuote =
      q"override def row(t: ${dataType.tpe}): org.apache.spark.sql.Row = org.apache.spark.sql.Row.merge(..$membersRows)"

    val membersFields =
      members.map { mem =>
        val memberType = mem.typeSignature.resultType
        val memberName = s"${mem.name.toTermName.toString}"
        q"implicitly[_root_.row.FlatRow[$memberType]].schema.fields.map(_root_.row.FlatRow.prependFieldPrefix($memberName))"
      }.reduce[Tree]{ case (fieldsA, fieldsB) => q"$fieldsA ++ $fieldsB" }
    val schemaQuote =
      q"override def schema: org.apache.spark.sql.types.StructType = org.apache.spark.sql.types.StructType($membersFields)"

    c.Expr[FlatRow[D]](
      q"new _root_.row.FlatRow[${dataType.tpe}] { $flatQuote; $rowQuote; $schemaQuote }"
    )
  }
  // scalastyle:on multiple.string.literals
}
