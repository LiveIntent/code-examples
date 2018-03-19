package com.liveintent.util
import scala.reflect.runtime.universe.{TypeTag,typeOf}
import org.roaringbitmap.RoaringBitmap
import org.joda.time.LocalDate
import com.liveintent.dwh.common.fields.{Field,EnumField}

/**
 * Type class for estimating memory size of runtime objects. Mainly useful for hunting for outliers.
 * 
 */



/**
 * Inner type class for SizeEstimator supporting auto-derivation
 * @author holbech
 *
 * @param <T>
 */
trait InnerSizeEstimator[T] extends Serializable {
  def apply(instance:T): Long
}

object InnerSizeEstimator {

  def constant[T] = new InnerSizeEstimator[T] {
    def apply(instance:T) = 8L
  } 
  
  import shapeless._
  import labelled.{field,FieldType}
  
  implicit val enumSizeEstimator = constant[Enumeration]
  implicit val intSizeEstimator = constant[Int]
  implicit val floatSizeEstimator = constant[Float]
  implicit val doubleSizeEstimator = constant[Double]
  implicit val longSizeEstimator = constant[Long]
  implicit val booleanSizeEstimator = constant[Boolean]
  implicit def enumFieldSizeEstimator[E<:EnumField] = constant[E]
  
  implicit def fieldSizeEstimator[V,F<:Field[V,F]](implicit i: InnerSizeEstimator[V]) = new InnerSizeEstimator[F] {
    def apply(instance:F) = i(instance.value)
  } 
  
  implicit val stringEstimator = new InnerSizeEstimator[String] {
    def apply(instance:String) = 8L + instance.length()*2
  } 
  
  implicit def createOption[U](implicit u: InnerSizeEstimator[U]): InnerSizeEstimator[Option[U]] = 
      new InnerSizeEstimator[Option[U]]{ 
        def apply(instance:Option[U]) = instance.foldLeft(8L)( (s,i) => s + u(i) )
      }
  
  implicit def createList[U](implicit u: InnerSizeEstimator[U]): InnerSizeEstimator[List[U]] = 
      new InnerSizeEstimator[List[U]]{ 
        def apply(instance:List[U]) = instance.foldLeft(8L)( (s,i) => s + 8L + u(i) )
      }
  
  implicit def createSet[U](implicit u: InnerSizeEstimator[U]): InnerSizeEstimator[Set[U]] = 
      new InnerSizeEstimator[Set[U]]{ 
        def apply(instance:Set[U]) = instance.foldLeft(8L)( (s,i) => s + 8L + u(i) )
      }
  
  implicit def createMap[X,Y](implicit x: InnerSizeEstimator[X], y: InnerSizeEstimator[Y]): InnerSizeEstimator[Map[X,Y]] = 
      new InnerSizeEstimator[Map[X,Y]]{ 
        def apply(instance:Map[X,Y]) = instance.foldLeft(8L)( (s,i) => s + 16L + x(i._1) + y(i._2) )
      }
  
  implicit val hnilEstimator = new InnerSizeEstimator[HNil] {
    def apply(instance:HNil) = 0L
  } 
  
  implicit def createHList[K<:Symbol,U,L<:HList]
                 (implicit u: InnerSizeEstimator[U], 
                           l: InnerSizeEstimator[L],
                           w: Witness.Aux[K]): InnerSizeEstimator[FieldType[K,U]::L] = 
      new InnerSizeEstimator[FieldType[K,U]::L] {
        def apply(instance:FieldType[K,U]::L) = 8L + u(instance.head) + l(instance.tail)
      }
  
  case class CreateProduct[U<:Product: TypeTag,L<:HList]( u : String)(implicit gen: LabelledGeneric.Aux[U,L], l: InnerSizeEstimator[L]) extends InnerSizeEstimator[U] { 
    require(!u.contains("<"), u + " is not a valid type name")
    def apply(instance:U) = l(gen.to(instance))
  }

  implicit def createProduct[U<:Product,L<:HList](implicit u : TypeTag[U], gen: LabelledGeneric.Aux[U,L], l: Lazy[InnerSizeEstimator[L]]): InnerSizeEstimator[U] = {
    implicit val inner = l.value
    CreateProduct[U,L](u.toString)
  }

}

/**
 * Type class for estimating runtime memory requirement of objects of type T
 * @author holbech
 *
 * @param <T>
 */
trait SizeEstimator[T] extends Serializable { 
  def apply(instance:T): Long
}

object SizeEstimator {

  case class CreateSizeEstimator[T](tpe: String)(implicit i:InnerSizeEstimator[T]) extends SizeEstimator[T] {
    def apply(instance:T): Long = i(instance)
  }
  
  def create[T:TypeTag](implicit i: InnerSizeEstimator[T]) = new CreateSizeEstimator[T](implicitly[TypeTag[T]].toString())
  
}
