package com.mojn.dwh.external.jobs

import com.liveintent.dwh.external.sources.EmailHashToIdlSource
import com.liveintent.dwh.external.models.EmailHashToIdl
import com.twitter.scalding.typed.MultiJoin
import com.liveintent.dwh.partners.liveramp.sources.IDLMasterSource
import com.liveintent.dwh.partners.oracle.sources.OracleMasterSource

/**
 * updates the master idl to email hash file located in external-dwh-liveintent-com
 * with the daily master delta that are delivered by liveramp
 */

class UpdateLiverampIDLJob(override val args: com.twitter.scalding.Args) extends com.mojn.util.ScaldingDWHJob(args) {

  
    val currentIdls = EmailHashToIdlSource(jobDates.dayBefore)
      .map { case eti@EmailHashToIdl(emailHash,_,_) => (emailHash,eti)}
  
    val newIdls = (if( args.optional("process-interval").isDefined || args.optional("process-interval-lr").isDefined ){ 
      IDLMasterSource()
    } else {
      IDLMasterSource(jobDates.lastDay.dayBefore)
    })
      .flatMap{ _.cleanedMaster() }
      .groupBy(_.getEmailHash)
      .reduce( _ + _ )      

    val oracleIdls = (if( args.optional("process-interval").isDefined || args.optional("process-interval-odc").isDefined ){ 
      OracleMasterSource()
    } else {
      OracleMasterSource(jobDates.lastDay.dayBefore)
    })
        .map{ om => (om.md5,om)}
    
    MultiJoin.outer(currentIdls,newIdls,oracleIdls)
      .map {
        case (emailHash,(None,None,Some(newOracleMaster))) => EmailHashToIdl.apply(newOracleMaster)
        case (emailHash,(None,Some(newIdlMaster),None)) => EmailHashToIdl.apply(newIdlMaster)
        case (emailHash,(None,Some(newIdlMaster),Some(newOracleMaster))) => (EmailHashToIdl.apply(newIdlMaster) + newOracleMaster)
        case (emailHash,(Some(currentIdl),None,None)) => currentIdl
        case (emailHash,(Some(currentIdl),Some(newIdlMaster),None)) => (currentIdl + newIdlMaster)
        case (emailHash,(Some(currentIdl),None,Some(newOracleMaster))) => (currentIdl + newOracleMaster)
        case (emailHash,(Some(currentIdl),Some(newIdlMaster),Some(newOracleMaster))) => (currentIdl + newIdlMaster + newOracleMaster)
        case (emailHash,t) => sys.error(s"it is not possible to merge $t")
      }
      .writeTo(EmailHashToIdlSource(jobDates.lastDay))
  
    
}

