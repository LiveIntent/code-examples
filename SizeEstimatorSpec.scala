package com.liveintent.util

import org.specs2.mutable._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SizeEstimatorSpec extends Specification {

  case class TestModel(a: String, b: Int, c: List[Int])
  
  "SizeEstimator" should {
    
    "work" in {
      val estimator = SizeEstimator.create[TestModel]
      estimator(TestModel("blabla", 4, (1 to 8).toList)) === 188
    }
    
  }
  
}
