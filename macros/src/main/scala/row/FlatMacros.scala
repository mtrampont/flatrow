package row

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.{Context => BlackContext}

object FlatMacros {

  implicit def flatRowCaseClass[D]: FlatRow[D] = macro flatRowImpl[D]

  // Multiple string literals check does not work well with interpolated strings (e.g. quasiquotes)
  // scalastyle:off multiple.string.literals
  def flatRowImpl[D: c.WeakTypeTag](c: BlackContext): c.Expr[FlatRow[D]] = {
    import c.universe._ // scalastyle:ignore

    val dataType = implicitly[c.WeakTypeTag[D]]
    val members = dataType.tpe.members.sorted.filter(s => s.isMethod && s.asMethod.isCaseAccessor)

    val membersFlats =
      members.map { mem =>
        val memberType = mem.typeSignature.resultType
        q"implicitly[FlatRow[$memberType]].flat(t.$mem)"
      }.fold(q"""Vector.empty[String]"""){ case (accumTree, flatMem) => q"$accumTree ++ $flatMem" }
    val flatQuote = q"override def flat(t: ${dataType.tpe}): Vector[String] = $membersFlats"

    val membersRows =
      members.map { mem =>
        val memberType = mem.typeSignature.resultType
        q"implicitly[FlatRow[$memberType]].row(t.$mem)"
      }
    val rowQuote = q"override def row(t: ${dataType.tpe}): Row = Row.merge(..$membersRows)"

    val membersFields =
      members.map { mem =>
        val memberType = mem.typeSignature.resultType
        val memberName = s"${mem.name.toTermName.toString}"
        q"implicitly[FlatRow[$memberType]].schema.fields.map(prependFieldPrefix($memberName))"
      }.reduce[Tree]{ case (fieldsA, fieldsB) => q"$fieldsA ++ $fieldsB" }
    val schemaQuote = q"override def schema: StructType = StructType($membersFields)"

    c.Expr[FlatRow[D]](q"new FlatRow[${dataType.tpe}] { $flatQuote; $rowQuote; $schemaQuote }")
  }
  // scalastyle:on multiple.string.literals
}
