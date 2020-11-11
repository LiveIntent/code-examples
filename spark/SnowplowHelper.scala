package com.liveintent.dwh.fragments

import com.liveintent.dwh.logs.snowplow.models.Snowplow
import org.apache.spark.rdd.RDD

/**
 * Snowplow is a model for evidence data from tracking user activity.
 * A row in Snowplow is a result of a call to a JS-function on the pubvertizer page.
 * domain_userid is the first party cookie related to the event
 * lidid is the third party cookie related to the event
 */
object SnowplowHelper {

  /**
   * we have seen snowplow data that originates from scrapers
   * so we simply remove all those traffics
   */
  private def removeScrapers[P](data: RDD[P], snowplowExtractor: P => Snowplow): RDD[P] = {

    data.map { p => (snowplowExtractor(p).domain_userid.value, p) }
      .leftOuterJoin(
        data
          .map(snowplowExtractor)
          .filter { s => s.lidid.nonEmpty && s.domain_userid.nonEmpty }
          .map(s => (s.domain_userid.value, s.lidid.value))
          .groupBy(_._1)
          .mapValues { pairs => {
            val firstElem = pairs.next()
            pairs.dropWhile(_ == firstElem).take(1)
          }
          }
          .map { case (domain_userid, _) => (domain_userid, 1) }
      )
      .flatMap {
        case (_, (s, None)) => Some(s)
        case (_, (s, Some(_))) => None
      }
  }
}


