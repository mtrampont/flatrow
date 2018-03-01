package common

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{FlatSpec, Inside, Matchers, OptionValues}

abstract class UnitTest extends FlatSpec with Matchers with OptionValues with Inside with TypeCheckedTripleEquals
