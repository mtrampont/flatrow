package row

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.{Context => BlackContext}

object FlatMacros {

  implicit def flatRowCaseClass[D]: FlatRow[D] = macro flatRowImpl[D]

  def flatRowImpl[D: c.WeakTypeTag](c: BlackContext): c.Expr[FlatRow[D]] = {
    import c.universe._

    val dataType = implicitly[c.WeakTypeTag[D]]
    val members = dataType.tpe.members.sorted.filter(s => s.isMethod && s.asMethod.isCaseAccessor)

    val membersFlats =
      members.map { mem =>
        val memberType = mem.typeSignature.resultType
        q"implicitly[FlatRow[$memberType]].flat(t.$mem)"
      }.fold(q"""Vector.empty[String]"""){ case (accumTree, flatMem) => q"$accumTree ++ $flatMem" }
    val flatQuote = q"override def flat(t: SampleData): Vector[String] = $membersFlats"

    val membersRows =
      members.map { mem =>
        val memberType = mem.typeSignature.resultType
        q"implicitly[FlatRow[$memberType]].row(t.$mem)"
      }
    val rowQuote = q"override def row(t: SampleData): Row = Row.merge(..$membersRows)"

    val membersFields =
      members.map { mem =>
        val memberType = mem.typeSignature.resultType
        val memberName = s"${mem.name.toTermName.toString}"
        q"implicitly[FlatRow[$memberType]].schema.fields.map(prependFieldPrefix($memberName))"
      }.reduce[Tree]{ case (fieldsA, fieldsB) => q"$fieldsA ++ $fieldsB" }
    val schemaQuote = q"override def schema: StructType = StructType($membersFields)"

    c.Expr[FlatRow[D]](q"new FlatRow[SampleData] { $flatQuote; $rowQuote; $schemaQuote }")
  }
}
