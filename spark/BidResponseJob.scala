package com.liveintent.dwh.logs.jobs

import com.liveintent.dwh.dmp.models._
import com.liveintent.dwh.dmp.sources.BidderGroupInfoSource
import com.liveintent.dwh.spark.util.{SparkArgs, SparkDwhJob}
import com.liveintent.dwh.userver.sources.ImpressionSource
import com.liveintent.util.logparser.LogParser
import com.twitter.algebird.Moments
import org.apache.spark.rdd.RDD


class BidResponsesJob(override val args: SparkArgs) extends SparkDwhJob(args) {

  import BidResponsesJob._

  /**
   * Impression is the number of times an ad is displayed to a person/seen by a person.
   * An impression is logged when ads are directly sent to a person, populated in an email, reopened, or forwarded to another person.
   * Impressions in our system are measured in terms of all impressions, first impressions, and fraud impressions.
   */
  private lazy val impressions = ImpressionSource()
    .filter { i => i.isFirstImpression }
    .groupBy { e => (e.decisionId.getOrElse(""), e.redecisionAttempt.getOrElse(-1)) }
    .mapValues(_.take(1))
    .map( _._2 )

  /**
   * A broken drop is defined as any scenario where key values in our livetags are not populated in accordance with defined standards.
   * We experience relatively high discrepancies across all demand sources from direct campaigns to DSP integrations.
   * There are many causes for this and a key one is broken drops
   */
  private lazy val brokenDropTriplets = impressions.filter( _.isBrokenDrop )
    .map( i => (Seq(i.md5,i.sh1,i.sh2).flatMap( e => e ).filter( e => e != null && !e.isEmpty ).headOption.getOrElse(""),
      i.templateId.getOrElse(-1),
      i.placementId) )
    .distinct
    .collect()
    .toSet

  private lazy val purchaseGroups = impressions
    .map(i => {
      val keys = (
        Seq(i.md5, i.sh1, i.sh2).flatMap(e => e).filter(e => e != null && !e.isEmpty).headOption.getOrElse(""),
        i.templateId.getOrElse(-1),
        i.placementId)
      if (brokenDropTriplets.contains(keys)) i.copy(isBrokenDrop = true) else i
    })
    .addPrefixGroup({ e => (e.bidderId, e.emailhash.map( _.value )) })
    .getGroupAggregates(
      momentsColumns= {
        case i => Map(
          "spend" -> i.advertiserSpent.getOrElse(0.0),
          "clearingPrice" -> i.clearingPrice.getOrElse(0.0),
          "expProfit" -> Seq(Some(0.0), i.actualDSPFee, i.actualPGMFee, i.actualSSPFee, i.thirdpartyMargin).flatMap(e => e).sum
        )
      },
      sumColumns={ case i => if (i.bidderId.isDefined) Map("isRtb"->1) else Map() },
      countColumns= {
        case i => Map(
          "pricingTypes" -> i.pricingType.filterNot(_.isEmpty).getOrElse("UNKNOWN-PRICINGTYPE"),
          "deviceTypes" -> i.userAgent.map(LogParser.parseUa(_).getDeviceCategory().getName()).filterNot(dt => dt.isEmpty || dt.toLowerCase() == "unknown").getOrElse("UNKNOWN-DEVICETYPE"),
          "oss" -> i.userAgent.map(LogParser.parseUa(_).getOperatingSystem().getFamilyName()).filterNot(os => os.isEmpty || os.toLowerCase() == "unknown").getOrElse("UNKNOWN-OS"),
          "browsers" -> i.userAgent.map(LogParser.parseUa(_).getFamily().getName()).filterNot(b => b.isEmpty || b.toLowerCase() == "unknown").getOrElse("UNKNOWN-BROWSER"),
          "sourcesImp" -> i.buyeruidSource.filterNot(_.isEmpty).getOrElse("UNKNOWN-UIDSOURCE")
        )
      }
    )

  private lazy val processingDate = jobDates.lastDay.toStrings().head

  purchaseGroups
    .map{ case (PrefixGroup(bidderId,prefixGroup), s) =>
      (bidderId, List(BidderGroupInfo(
        date=processingDate,
        definition=BidderGroupDefinition(bidderId, prefixGroup, "NO-LABEL", "NO-DESCRIPTION"),
        bids=s.meanStatistic("bidPrice"),
        nonZeroBids=s.meanStatistic("nonZeroBidPrice"),
        largeBids=s.meanStatistic("largeBidPrice"),
        spend=s.meanStatistic("spend"),
        clearingPrice=s.meanStatistic("clearingPrice"),
        expectedProfit=s.meanStatistic("expProfit"),
        clicks=s.proportionStatistic("clicks", "spend"),
        conversions=s.proportionStatistic("conversions", "spend"),
        rtbWins=s.proportionStatistic("isRtb", "spend"),
        sources=(if (bidderId == "ALL") s.categoryStatistic("sourcesImp", "spend") else s.categoryStatistic("sourcesBids", "bidPrice")),
        pricingTypes=s.categoryStatistic("pricingTypes", "spend"),
        responses=s.proportionStatistic("nonZeroBidPrice", "bidPrice"),
        deviceTypes=s.categoryStatistic("deviceTypes", "spend"),
        oss=s.categoryStatistic("oss", "spend"),
        browsers=s.categoryStatistic("browsers", "spend"),
        idaasIndices=s.categoryStatistic("idaasIndex", "bidPrice")
      ) )) }
    .reduceByKey{case (bgInfo1, bgInfo2) => {
      val bgInfos1 = bgInfo1.reduce((b1, b2) => b1.crossWith(b2))
      val bgInfos2 = bgInfo2.reduce((b1, b2) => b1.crossWith(b2))
      bgInfo1.map(bgi1 => bgi1.crossWith(bgInfos2)) ++ bgInfo2.map(bgi2 => bgi2.crossWith(bgInfos1))
    }}
    .flatMap(_._2)
    .coalesce(1)
    .writeTo(BidderGroupInfoSource())

}

object BidResponsesJob {

  private[BidResponsesJob] case class PrefixGroup(bidder: String, group: String) extends Ordered[PrefixGroup] {
    def compare(that: PrefixGroup): Int = (bidder + "#" + group).compare(that.bidder + "#" + that.group)
  }

  private implicit class PrefixAdder[T](val data: RDD[T]) extends Serializable
  {
    def addPrefixGroup(extractor: (T)=>(Option[Int],Option[String]), noAll: Boolean=false, noAllRtb: Boolean=false): RDD[(PrefixGroup,T)] = {
      val unacceptableBidders = (noAll, noAllRtb) match {
        case (true,true) => List("ALL","ALL-RTB")
        case (false,true) => List("ALL-RTB")
        case (true,false) => List("ALL")
        case (false,false) => Nil
      }
      data.flatMap( t =>
        (extractor(t) match {
          case (None, None) => Seq(("ALL", "NO-HASH"))
          case (None, Some(hash)) => Seq(("ALL", "OUTSIDE-GROUPS"))
          case (Some(bidder), None) => Seq(
            ("ALL", "NO-HASH"),
            ("ALL-RTB", "NO-HASH"),
            (bidder.toString(), "NO-HASH")
            (bidder.toString(), "UNKNOWN-BIDDER")
          )
          case (Some(bidder), Some(hash)) => Seq(
            ("ALL", "OUTSIDE-GROUPS"),
            ("ALL-RTB", "OUTSIDE-GROUPS"),
            (bidder.toString(), "UNKNOWN-BIDDER")
          )
        })
          .filterNot( e => unacceptableBidders.contains(e._1) )
          .flatMap( pg => Seq((PrefixGroup(pg._1, pg._2), t), (PrefixGroup(pg._1, "ALL"), t)) )
      )
    }
  }

  import com.twitter.algebird._

  private implicit def tuple3Semigroup[A:Semigroup,B:Semigroup,C:Semigroup]: Semigroup[(A,B,C)] = new Semigroup[(A,B,C)] {
    def plus(l: (A,B,C), r: (A,B,C)): (A,B,C) = (implicitly[Semigroup[A]].plus(l._1, r._1),
      implicitly[Semigroup[B]].plus(l._2, r._2),
      implicitly[Semigroup[C]].plus(l._3, r._3))
  }

  private val smAggregator = SketchMap.aggregator[String, Long](SketchMapParams[String](1, 0.001, 1E-10, 50){ s => s.toArray.map( _.toByte ) })

  private implicit val smSemigroup: Semigroup[SketchMap[String,Long]] = new Semigroup[SketchMap[String,Long]] {
    def plus(l: SketchMap[String,Long], r: SketchMap[String,Long]) = smAggregator.reduce(l, r)
  }

  private implicit class MapAdd[X,Y](m: Map[X,Y]) extends Serializable {
    def +|+(other: Map[X,Y]) = m ++ other.map( e => if (m.contains(e._1)) throw new RuntimeException(s"Found key ${e._1} in both maps") else e )
  }

  private[BidResponsesJob] implicit class Aggregator[T](val data: RDD[(PrefixGroup,T)]) extends Serializable
  {
    def getGroupAggregates(momentsColumns: (T)=>Map[String,Double] = { e => Map() },
                           sumColumns: (T)=>Map[String,Int] = { e => Map() },
                           countColumns: (T)=>Map[String,String] = { e => Map() }): RDD[(PrefixGroup,GroupStatistics)] = {
      data.filterNot( _._1.group.isEmpty() )
        .map{ case (g,p) => (g, (momentsColumns(p).mapValues(Moments(_)),
          sumColumns(p).mapValues(_.toLong),
          countColumns(p).mapValues( v => smAggregator.prepare((v,1L)) )
        )) }
        .reduceByKey((columns1, columns2) => (columns1._1++columns2._1, columns1._2++columns2._2, columns1._3++columns2._3))
        .map{ case (g,r) => (g, GroupStatistics(
          r._1,
          r._2,
          r._3.mapValues( sm => sm.heavyHitterKeys
            .map( k => (k, smAggregator.monoid.frequency(sm,k)) )
            .toMap )
        ) ) }
    }
  }

  private[BidResponsesJob] case class GroupStatistics(moments: Map[String,Moments], sums: Map[String,Long], counts: Map[String,Map[String,Long]]) {

    def ++(other: GroupStatistics): GroupStatistics = copy(
      moments=moments +|+ other.moments,
      sums=sums +|+ other.sums,
      counts=counts +|+ other.counts
    )

    def meanStatistic(name: String) = moments.get(name).map( m => GroupMeanStatistic(m.mean, m.count, Some(m.stddev).filterNot( _.isNaN )) )

    private def getCount(name: String) = sums.get(name).orElse(moments.get(name).map( m => m.count ))

    def proportionStatistic(name: String, totalName: String) = getCount(totalName).map( t => GroupProportionStatistic(getCount(name).getOrElse(0L), t) )

    def categoryStatistic(name: String, totalName: String) = counts.get(name).zip(getCount(totalName))
      .map{ case (c,t) => GroupCategoryStatistic(c,t) }
      .headOption
  }


}
