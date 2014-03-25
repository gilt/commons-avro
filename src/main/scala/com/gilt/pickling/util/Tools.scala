package com.gilt.pickling.util

import scala.reflect.runtime.universe.{TypeRef, Type, ClassSymbol, ExistentialType}

object Tools {
  //TODO This is from scala.pickling.internal which is scooped as private.
  //TODO Optimisation is to cache type => String
  implicit class RichTypeFIXME(tpe: Type) {
    import scala.reflect.runtime.universe.definitions.ArrayClass
    def key: String = {
      tpe.normalize match {
        case ExistentialType(tparams, TypeRef(pre, sym, targs))
          if targs.nonEmpty && targs.forall(targ => tparams.contains(targ.typeSymbol)) =>
          TypeRef(pre, sym, Nil).key
        case TypeRef(pre, sym, targs) if pre.typeSymbol.isModuleClass =>
          sym.fullName +
            (if (sym.isModuleClass) ".type" else "") +
            (if (targs.isEmpty) "" else targs.map(_.key).mkString("[", ",", "]"))
        case _ =>
          tpe.toString
      }
    }
    def isEffectivelyPrimitive: Boolean = tpe match {
      case TypeRef(_, sym: ClassSymbol, _) if sym.isPrimitive => true
      case TypeRef(_, sym, eltpe :: Nil) if sym == ArrayClass && eltpe.isEffectivelyPrimitive => true
      case _ => false
    }
  }
}
