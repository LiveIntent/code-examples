package com.liveintent.dwh.logs.jobs

import com.liveintent.dwh.common.jobs.SystemSetup
import com.liveintent.dwh.dmp.models._
import com.liveintent.dwh.dmp.sources.BidderGroupInfoSource
import com.liveintent.dwh.idaas.sources.EmailBuildMappingSelectorsSource
import com.liveintent.dwh.logs.Logs
import com.liveintent.dwh.mapping.selection.MappingSelector
import com.liveintent.dwh.paths.S3
import com.liveintent.dwh.spark.util.{SparkArgs, SparkDwhJob}
import com.liveintent.dwh.userver.sources.{ImpressionSource, RtbResponseSource}
import com.liveintent.util.SourceToTraversable
import com.liveintent.util.logparser.LogParser
import com.twitter.algebird.Moments
import org.apache.spark.storage.StorageLevel


class BidResponsesJob(override val args: SparkArgs) extends SparkDwhJob(args) {

  import BidResponsesJob._

  private lazy val impressions = ImpressionSource()
    .map{ _.sanitizeToPositive }
    .filter { i => i.isFirstImpression && i.demandType == Some("default") }
    .groupBy { e => (e.decisionId.getOrElse(""), e.redecisionAttempt.getOrElse(-1)) }
    .mapValues(_.take(1))
    .map( _._2 )

  private lazy val brokenDropTriplets = impressions.filter( _.isBrokenDrop )
    .map( i => (Seq(i.md5,i.sh1,i.sh2).flatMap( e => e ).filter( e => e != null && !e.isEmpty ).headOption.getOrElse(""),
      i.templateId.getOrElse(-1),
      i.placementId) )
    .distinct
    .collect()
    .toSet

  private lazy val responses = (RtbResponseSource())
    .filter( _.decisionId.isDefined )
    .map( r => {
      val keys = (
        Seq(r.user.md5,r.user.sh1,r.user.sh2).flatMap( e => e ).filter( e => e != null && !e.isEmpty ).headOption.getOrElse(""),
        r.offer.templateId,
        r.offer.placementId
      )
      if (brokenDropTriplets.contains(keys)) r.copy(user=r.user.copy(md5=None,sh1=None,sh2=None)).asFlat
      else r.asFlat
    })
    .map(r => ((r.decisionId.get, r.redecisionAttempt.getOrElse(-1), r.bidderId, r.idaasIndex), r))
    .reduceByKey((r1, r2) => if (r1.bidPrice > r2.bidPrice) r1 else r2)
    .map(_._2)
    .persist(StorageLevel.DISK_ONLY)

  lazy implicit val (mappingSelector, selectorChanges) = BidResponsesJob.getUsedMappingSelectors(endDateAsString)

  private lazy val bidLabels = responses
    .filter( _.bidPrice > 0.0 )
    .map( r => (r.bidderId, Moments(r.bidPrice)) )
    .reduceByKey(MomentsGroup.plus(_,_))
    .map{ case (bidderId, moments) =>
      if (moments.variance > 0.0)
        (bidderId, Array((0.0, "low"),(moments.mean,"avg+"),(moments.mean+moments.stddev,"high"),(moments.mean+(2*moments.stddev),"extreme")))
      else
        (bidderId, Array((0.0, "unknown")))
    }
    .collectAsMap()

  private lazy val sizedBids = responses.map { r => (r,
    if (r.bidPrice == 0.0) "zero"
    else if (bidLabels.contains(r.bidderId))
      bidLabels.apply(r.bidderId).takeWhile(e => r.bidPrice > e._1).lastOption.map(_._2).getOrElse("zero")
    else "unknown"
  )}

  private lazy val bidsWithSizeAndGroup = sizedBids.addPrefixGroup({ e => (Some(e._1.bidderId), e._1.hash.map( _.value )) }, noAll=true)

  private lazy val bidStatistics = responses
    .map { r =>(r,
      if (r.bidPrice == 0.0) "zero"
      else if (bidLabels.contains(r.bidderId))
        bidLabels.apply(r.bidderId).takeWhile(e => r.bidPrice > e._1).lastOption.map(_._2).getOrElse("zero")
      else "unknown"
    )}
    .addPrefixGroup({ e => (Some(e._1.bidderId), e._1.hash.map( _.value )) }, noAll=true)
    .getGroupAggregates(
    momentsColumns={ case (r,s) => s match {
      case "unknown" => Map("bidPrice" -> r.bidPrice)
      case "zero" => Map("bidPrice" -> r.bidPrice)
      case "low" => Map("bidPrice" -> r.bidPrice, "nonZeroBidPrice" -> r.bidPrice)
      case _ => Map("bidPrice" -> r.bidPrice, "nonZeroBidPrice" -> r.bidPrice, "largeBidPrice" -> r.bidPrice)
    } },
    countColumns={ case (r,s) => Map(
      "bidSizes"->s,
      "idaasIndex"->r.idaasIndex.map( _.toString() ).getOrElse("None"),
      "sourcesBids"->r.bidderUUIDSource.toString()
    ) }
  )

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

  (bidStatistics ++ purchaseGroups)
    .reduceByKey(_ ++ _)
    .map{ case (PrefixGroup(bidderId,prefixGroup), s) =>
      (bidderId, List(BidderGroupInfo(date=processingDate,
        definition=(if (Seq("ALL","ALL-RTB").contains(bidderId)) MappingSelector.CommonGroupings else bidderId) match {
          case lookupBidderId =>
            BidderGroupDefinition(bidderId,
              prefixGroup,
              mappingSelector.getGroupLabel(prefixGroup, lookupBidderId).filter( e => prefixGroup.toLowerCase() == prefixGroup && !e.isEmpty ).getOrElse("NO-LABEL"),
              mappingSelector.getGroupDescription(prefixGroup, lookupBidderId).filter( e => prefixGroup.toLowerCase() == prefixGroup && !e.isEmpty ).getOrElse("NO-DESCRIPTION"),
              selectorChanges.groupRemoved(lookupBidderId, prefixGroup),
              selectorChanges.additionalChangesInDay)
        },
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

  private implicit class PrefixAdder[T](val pipe: TypedPipe[T])(implicit mappingSelector:MappingSelector) extends Serializable
  {
    def addPrefixGroup(extractor: (T)=>(Option[Int],Option[String]), noAll: Boolean=false, noAllRtb: Boolean=false): TypedPipe[(PrefixGroup,T)] = {
      val unacceptableBidders = (noAll, noAllRtb) match {
        case (true,true) => List("ALL","ALL-RTB")
        case (false,true) => List("ALL-RTB")
        case (true,false) => List("ALL")
        case (false,false) => Nil
      }
      pipe.flatMap( t => (extractor(t) match {
        case (None, None) => Seq(("ALL", "NO-HASH"))
        case (None, Some(hash))  => Seq(("ALL", mappingSelector.getGroupPrefix(hash, MappingSelector.CommonGroupings).getOrElse("OUTSIDE-GROUPS")))
        case (Some(bidder), None) if mappingSelector.excludedFromWatermarking.contains(bidder.toString()) =>
          Seq((bidder.toString(), if (mappingSelector.groupingNames.contains(bidder.toString())) "NO-HASH" else "UNKNOWN-BIDDER"))
        case (Some(bidder), None) => Seq(("ALL", "NO-HASH"),
          ("ALL-RTB", "NO-HASH"),
          (bidder.toString(), if (mappingSelector.groupingNames.contains(bidder.toString())) "NO-HASH" else "UNKNOWN-BIDDER"))
        case (Some(bidder), Some(hash)) if mappingSelector.excludedFromWatermarking.contains(bidder.toString()) =>
          Seq((bidder.toString(), mappingSelector.getGroupPrefix(hash, bidder.toString()).getOrElse("UNKNOWN-BIDDER")))
        case (Some(bidder), Some(hash)) => Seq(("ALL", mappingSelector.getGroupPrefix(hash, MappingSelector.CommonGroupings).getOrElse("OUTSIDE-GROUPS")),
          ("ALL-RTB", mappingSelector.getGroupPrefix(hash, MappingSelector.CommonGroupings).getOrElse("OUTSIDE-GROUPS")),
          (bidder.toString(), mappingSelector.getGroupPrefix(hash, bidder.toString()).getOrElse("UNKNOWN-BIDDER")))
      }).filterNot( e => unacceptableBidders.contains(e._1) ).flatMap( pg => Seq((PrefixGroup(pg._1, pg._2), t), (PrefixGroup(pg._1, "ALL"), t)) ) )
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

  private[BidResponsesJob] implicit class Aggregator[T](val pipe: TypedPipe[(PrefixGroup,T)]) extends Serializable
  {
    def getGroupAggregates(momentsColumns: (T)=>Map[String,Double] = { e => Map() },
                           sumColumns: (T)=>Map[String,Int] = { e => Map() },
                           countColumns: (T)=>Map[String,String] = { e => Map() }):
    TypedPipe[(PrefixGroup,GroupStatistics)] =
      pipe.filterNot( _._1.group.isEmpty() )
        .map{ case (g,p) => ((g.bidder,g.group), (momentsColumns(p).mapValues(Moments(_)),
          sumColumns(p).mapValues(_.toLong),
          countColumns(p).mapValues( v => smAggregator.prepare((v,1L)) )
        )) }
        .group
        .sum
        .map{ case (g,r) => (PrefixGroup(g._1,g._2), GroupStatistics(
          r._1,
          r._2,
          r._3.mapValues( sm => sm.heavyHitterKeys
            .map( k => (k, smAggregator.monoid.frequency(sm,k)) )
            .toMap ))) }
  }

  private[BidResponsesJob] case class GroupStatistics(moments: Map[String,Moments], sums: Map[String,Long], counts: Map[String,Map[String,Long]]) {

    def ++(other: GroupStatistics): GroupStatistics = copy(moments=moments +|+ other.moments,
      sums=sums +|+ other.sums,
      counts=counts +|+ other.counts)

    def meanStatistic(name: String) = moments.get(name).map( m => GroupMeanStatistic(m.mean, m.count, Some(m.stddev).filterNot( _.isNaN )) )

    private def getCount(name: String) = sums.get(name).orElse(moments.get(name).map( m => m.count ))

    def proportionStatistic(name: String, totalName: String) = getCount(totalName).map( t => GroupProportionStatistic(getCount(name).getOrElse(0L), t) )

    def categoryStatistic(name: String, totalName: String) = counts.get(name).zip(getCount(totalName))
      .map{ case (c,t) => GroupCategoryStatistic(c,t) }
      .headOption
  }

  private lazy val usedMappingSelectors = com.rt.common.Amazon.findFilesOnS3(s"s3://logs-dwh-liveintent-com/used-mapping-selectors/*/*.gz")
    .map( s => (s, s.split("used-mapping-selectors/")(1).split("/")(0)) )
    .toList
    .sortBy( _._1 )

  private def getUsedMappingSelectorDef(storeUploadTime:String)(implicit log: org.slf4j.Logger, systemSetup : SystemSetup): Iterable[String] = {
    val candidates = usedMappingSelectors.takeWhile( _._2  <= storeUploadTime )
    candidates.filter( _._2 == candidates.last._2 )
      .toIterable
      .flatMap( s => SourceToTraversable(EmailBuildMappingSelectorsSource(s._2)(Logs(S3), implicitly))(
        implicitly,
        implicitly,
        implicitly).map( _.selectorLine ) )
  }

  private[jobs] case class SelectorChanges(removed:Set[(String,String)]=Set(), additionalChangesInDay:Int=0) {
    def groupRemoved(bidderId: String, prefix: String) = removed.contains((bidderId, prefix))
  }

  private[jobs] def getUsedMappingSelectors(date:String)(implicit log: org.slf4j.Logger, systemSetup : SystemSetup): (MappingSelector, SelectorChanges)  = {
    log.info(s"Looking for serialized mapping selectors for date ${date}")
    val (beforeDay, onOrAfterDay) = com.rt.common.Amazon.findFilesOnS3("s3://logs-dwh-liveintent-com/store-deploys/segment-data-*.tsv")
      .toList
      .map( s => s.split("segment-data-")(1).split("\\.")(0) )
      .partition( _ < date )
    val onDay = onOrAfterDay.filter( _.startsWith(date) )
    val mappingSelector = MappingSelector(getUsedMappingSelectorDef(beforeDay.max))
    if (onDay.isEmpty)
      (mappingSelector, SelectorChanges())
    else {
      val endOfDayMappingSelector =  MappingSelector(getUsedMappingSelectorDef(onDay.max))
      val groupsRemoved = mappingSelector.groupDifferences(endOfDayMappingSelector)
        .flatMap{ _ match {
          case (bidder, group, "NEW") => None
          case (bidder, group, "REMOVED") => Some((bidder, group.prefixStart))
        }
        }.toSet
      val othersOnDay = onDay.sorted.reverse.tail.size
      (mappingSelector, SelectorChanges(groupsRemoved, othersOnDay))
    }
  }

}
