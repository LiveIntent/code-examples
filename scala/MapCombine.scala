package com.mojn.util

import scala.collection.mutable.{ Map => MutableMap }

object MapCombine {
  
  def apply[K,V](m1:Map[K,V], m2:Map[K,V], merge: (V,V) => V): Map[K,V] = combine(m1,m2,merge)
  
  def combine[K,V](m1:Map[K,V], m2:Map[K,V], merge: (V,V) => V): Map[K,V] =
    (m1,m2) match {
      case (null,null) => Map()
      case (null, something) => something
      case (something, null) => something
      case (left, right) => left ++ right.map{ case (k,v) => (k, left.get(k).map( merge(_,v) ).getOrElse(v) ) }
    }
  
}
object MutableMapCombine {
  
  def apply[K,V](m1:MutableMap[K,V], m2:MutableMap[K,V], merge: (V,V) => V): MutableMap[K,V] = combine(m1,m2,merge)
  
  def combine[K,V](m1:MutableMap[K,V], m2:MutableMap[K,V], merge: (V,V) => V): MutableMap[K,V] =
    (m1,m2) match {
      case (null,null) => MutableMap()
      case (null, something) => something
      case (something, null) => something
      case (left, right) => left ++ right.map{ case (k,v) => (k, left.get(k).map( merge(_,v) ).getOrElse(v) ) }
    }
  
}