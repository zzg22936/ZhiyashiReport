package com.cfetsit.report

/**
  * Created by zzg on 2018/8/16.
  */
import java.text.SimpleDateFormat
import java.util.Date

import com.cfetsit.util.csvTokenUtil
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.phoenix.spark._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, Set}

/**
  * Created by zzg on 2018/8/14.
  * 使用 6位码标识机构。
  * 在ReportRefine基础上进行改动
  * 1.改进了从交易表中获得质押式交易休息的逻辑,bond_repo_deal表增加了mkt_id字段,与EV表的join逻辑进行了修改
  * 2.两个表字段长度变化
  * 3.增加计时信息
  * 4.自营->机构 日期统一放到读取数据端解决。2018/8/14
  * 5.解决 机构到根机构映射
  * 6.服务点阵图需要所有机构的 正回购加权价 和 逆回购的加权价
  *7.机构类型价差计算时，要保证每个机构都有正逆回购
  */

object ReportAnalysis {

  /*
     args[0]:zookeeperURL; args[1]=pd_d_path ; args[2] market_close_type_I;args[3]=member_ctgry_d; args[4]=after_hour_bond_type; args[5]=member_d_path ;
    ars[6]= MSTR_SLV_RL_TP_RL; args[7]= cim_ORG_RL;args[8]=trdx_deal_infrmn_rmv_d_path
    args[9]= bond_repo_deal_path; args[10]= bond_d;args[11]=ri_credit_rtng ;
    args[12]=dps_v_cr_dep_txn_dtl_data; args[13] = ev_cltrl_dtls
    */
  @transient lazy val  log = Logger.getLogger(this.getClass)
//@transient lazy val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf() //！！！集群上改！！！
 //   sparkConf.setMaster("local[2]").setAppName("report")
    val sc = new SparkContext(sparkConf)

   // val ZOOKEEPER_URL="127.0.0.1:2181"
   val ZOOKEEPER_URL=args(0)
    val conf = new Configuration()
    println("zookeeper url:"+args(0))
    log.info("zookeeper url:"+args(0))
    /*
   1、 清洗数据
     */
    val beginTime = System.currentTimeMillis()
    val pd_d_data = sc.textFile(args(1))
      .filter(_.length!=0).filter(!_.contains("PD")) //表名首字个缩写
      .filter(line=>{
      val lineList = csvTokenUtil.split(line)
      if(lineList.size() !=2){
        false
      }else{
        var flag = true
        for(i<-0 until lineList.size() if flag){
          if(lineList.get(i)==null || lineList.get(i).isEmpty){
            flag =false
          }
        }
        flag
      }
    })
      .map(line=>{
        val tmp =csvTokenUtil.split(line)
        (tmp.get(0),tmp.get(1))
      })
      . collectAsMap()

    val broadPD_D = sc.broadcast(pd_d_data)
    val time2 = System.currentTimeMillis()
    println("load PD_D cost "+ (time2-beginTime)/1000+" seconds"+" and read "+ pd_d_data.size+" records")
    log.info("load PD_D cost "+ (time2-beginTime)/1000+" seconds"+" and read "+ pd_d_data.size+" records")
    //step1:得到 market_close_type_I,选出属于质押式回购的机构类型
    val market_close_type_i_data = sc.textFile(args(2))
      .filter(_.length!=0).filter(!_.contains("MARKET"))
      .filter(line=>{
        val lineList = csvTokenUtil.split(line)
        if(lineList.size()!=3){
          false
        }else {
          var flag = true
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty){
              flag =false
            }
          }
          if(!lineList.get(0).equals("质押式回购")){
            flag =false
          }
          flag
        }
      }).map(line=>{
      val lineList = csvTokenUtil.split(line)
      var ins_show_name= lineList.get(2)
      val a = ins_show_name.indexOf("(")
      val b = ins_show_name.indexOf("（")
      val c = math.max(a,b)
      if(c>0){
        ins_show_name = ins_show_name.substring(0,c).trim
      }

      (lineList.get(1),ins_show_name)//只取后两个字段
    }).collectAsMap()
    val time3 = System.currentTimeMillis()
    println("load market_close_type_i cost "+ (time3-time2)/1000+" seconds"+
      " and read "+ market_close_type_i_data.size+" records")
    log.info("load market_close_type_i cost "+ (time3-time2)/1000+" seconds"+
      " and read "+ market_close_type_i_data.size+" records")

    val broadMARKET_CLOSE_TYPE_ID_DATA = sc.broadcast(market_close_type_i_data)

    //step1 : 将机构类型表的 类型名称映射为 可展示类型
    val mem_cannot_find_showname = sc.longAccumulator("mem_cannot_find_showname")
    val member_ctgry_d_data = sc.textFile(args(3))
      .filter(_.length!=0).filter(!_.contains("MEMBER")).filter(line=>{
      val lineList = csvTokenUtil.split(line)
      if(lineList.size() != 2){
        false
      }else{
        var flag = true
        for(i<-0 until lineList.size() if flag){
          if(lineList.get(i)==null || lineList.get(i).isEmpty){
            flag =false
          }
        }
        flag
      }
    }).map(line=>{
      val lineList = csvTokenUtil.split(line)
      val market_close_type_data = broadMARKET_CLOSE_TYPE_ID_DATA.value
      var ins_show_name = "None"
      if(!market_close_type_data.contains(lineList.get(1))){
        log.warn("market_close_type_data does not contain ins_type "+lineList.get(1))
        mem_cannot_find_showname.add(1l)
      }else{
        ins_show_name = market_close_type_data.get(lineList.get(1)).get//根据机构分类名得到展示端名字
      }
      //  val ins_show_name = market_close_type_data.get(lineList.get(1)).get
      (lineList.get(0),ins_show_name)
    }).filter(x=> !x._2.equals("None")) //过滤掉找不到内部名称的机构
      .collectAsMap()

    val time4 = System.currentTimeMillis()
    println("load member_ctgry_d cost "+ (time4-time3)/1000+" seconds"+
      " and read "+ member_ctgry_d_data.size+" records")
    log.info("load member_ctgry_d cost "+ (time4-time3)/1000+" seconds"+
      " and read "+ member_ctgry_d_data.size+" records")

    //读债券市场盘后类型
    val after_hours_bound_type_data = sc.textFile(args(4))
      .filter(_.length!=0).filter(!_.contains("MEMBER")).filter(line=>{
      val lineList = csvTokenUtil.split(line)
      var flag = true
      if(lineList.size() != 2){
        flag = false
      }else{
        for(i<-0 until lineList.size() if flag){
          if(lineList.get(i)==null || lineList.get(i).isEmpty){
            flag =false
          }
        }
      }
      flag
    }).map(line=> {
      val lineList = csvTokenUtil.split(line)
      val member_ctgry_id = lineList.get(1)
      var ins_show_name = lineList.get(0)
      val a = ins_show_name.indexOf("(")
      val b = ins_show_name.indexOf("（")
      val c = math.max(a,b)
      if(c>0){
        ins_show_name = ins_show_name.substring(0,c).trim
      }
      (member_ctgry_id, ins_show_name)
    }).collectAsMap()

    val time5 = System.currentTimeMillis()
    println("load after_hours_bound_type cost "+ (time5-time4)/1000+" seconds"+" and read "+ after_hours_bound_type_data.size+" records")
    log.info("load after_hours_bound_type cost "+ (time5-time4)/1000+" seconds"+" and read "+ after_hours_bound_type_data.size+" records")

    val broadAfterHoursBondType= sc.broadcast(after_hours_bound_type_data)
    val broadMEMER_CTGRY_D = sc.broadcast(member_ctgry_d_data)
    /*
    修改机构的member_d数据，增加机构类型名(可展示类型。) key(ip_id)机构ID
     */
    val mem_cannot_find_ctgryID = sc.longAccumulator("mem_cannot_find_ctgryID")
    val member_d_rdd = sc.textFile(args(5))
      .filter(_.length!=0).filter(!_.contains("MEMBER")).filter(line=>{
      var flag = true
      val lineList = csvTokenUtil.split(line)
      if(lineList.size() != 10){ flag= false}
      else{
        //修改原则
        for(i<-0 until lineList.size() if flag){
          if(lineList.get(i)==null || lineList.get(i).isEmpty){flag =false}
        }
        if(!lineList.get(7).equals("9999-12-31")){
          flag = false
        }
      }
      flag
    })
      .map(line=>{
        val lineList = csvTokenUtil.split(line)
        val ip_id = lineList.get(0)
        val member_ctgry_id = lineList.get(1)
      //  val member_cd = lineList.get(2)
        val full_nm  = lineList.get(3)
      //  val rt_member_cd = lineList.get(4)
     //   val rt_member_full_nm = lineList.get(5)
        val oracleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val efctv_from_dt = oracleDateFormat.parse(lineList.get(6))
        val efctv_to_dt = oracleDateFormat.parse(lineList.get(7))
        val unq_id_in_src_sys  = lineList.get(8)
        val member_ctgry_d = broadMEMER_CTGRY_D.value
        var ins_show_name = "None"
        if(!member_ctgry_d.contains(member_ctgry_id)){
          log.warn("member_ctgry_d doesnot contain member_ctgry_nm "+member_ctgry_id)
          mem_cannot_find_ctgryID.add(1l)
        }else{
          ins_show_name = member_ctgry_d.get(member_ctgry_id).get
        }
        //质押式市场 机构展示名称
        var bond_after_show_name = "None"
        if(!broadAfterHoursBondType.value.contains(member_ctgry_id)){
          log.warn("broadAfterHoursBondType does not contain member_ctgry_id:"+member_ctgry_id)
        }else{
          bond_after_show_name = broadAfterHoursBondType.value.get(member_ctgry_id).get
        }
        // 1      2               3        4            5            6                7             8
        (ip_id,member_ctgry_id,full_nm,efctv_from_dt,efctv_to_dt, unq_id_in_src_sys,ins_show_name,bond_after_show_name)
      }).filter(record=>{
      var flag = true
      if(record._7.equals("None")|| record._8.equals("None")){
        flag = false
      }
      flag
    })
      .cache()
    val time6 = System.currentTimeMillis()
    println("load member_d cost "+ (time6-time5)/1000+" seconds")
    log.info("load member_d cost "+ (time6-time5)/1000+" seconds")

    //对每个机构计算映射6位码、机构全名、和 质押式 盘后市场类型、债券盘后市场类型
    // 1        2             3          4              5               6                  7             8
   // (ip_id,member_ctgry_id,full_nm,efctv_from_dt,efctv_to_dt, unq_id_in_src_sys,ins_show_name,bond_after_show_name)
    member_d_rdd.map(record=>{
      val member_uniq = record._6
      val full_nm  = record._3
      val ins_show_name = record._7
      val bond_after_show_name = record._8
      (member_uniq,full_nm,ins_show_name,bond_after_show_name)
    }).saveToPhoenix("MEMBER_SKETCH_INFO",Seq("UNQ_ID_IN_SRC_SYS","FULL_NM",
      "INS_SHOW_NAME","BOND_AFTER_SHOW_NAME"),conf,Some(ZOOKEEPER_URL))
    println("Save to MEMBER_SKETCH_INFO succeed!")
    log.info("Save to MEMBER_SKETCH_INFO succeed!")
    val memberId2Uniq = member_d_rdd.map(record=>{
      val ip_id = record._1
      val unq_id = record._6
      (ip_id,unq_id)
    }).collectAsMap()
    println("memberId2Uniq dataset size is "+ memberId2Uniq.size)
    log.info("memberId2Uniq dataset size is "+ memberId2Uniq.size)

    val broadMemberId2Uniq = sc.broadcast(memberId2Uniq)
    val memberUniq2Info = member_d_rdd.map(record=>{
      val full_nm = record._3
      val unq_id = record._6
      val ins_show_name = record._7
      (unq_id,(full_nm,ins_show_name))
    }).collectAsMap()
    val broadMemerUniq2Info = sc.broadcast(memberUniq2Info)
    val time7 = System.currentTimeMillis()
    println("collect  broadMemberStaticInfo  cost "+ (time7-time6)/1000+" seconds")
    log.info("collect  broadMemberStaticInfo cost "+ (time7-time6)/1000+" seconds")

    //找出主从关系
    val cim_mster_slv_rl_tp_rl_data = sc.textFile(args(6))
      .filter(_.length!=0).filter(!_.contains("MSTR")).filter(line=>{
      val lineList = csvTokenUtil.split(line)
      var flag = true
      if(lineList.size() != 4){flag = false}
      else{
        for(i<-0 until lineList.size() if flag){
          if(lineList.get(i)==null || lineList.get(i).isEmpty) {flag =false}
        }
        if(!lineList.get(0).equals("1")){
          flag =false
        }
        if(!lineList.get(2).startsWith("9999-12-31")){
          flag = false
        }
      }
      flag
    }).map(line=>{
      val lineList = csvTokenUtil.split(line)
      lineList.get(1)
    }).collect().toSet
    val broadCIM_MSTER_SLV_RL = sc.broadcast(cim_mster_slv_rl_tp_rl_data)

    val cim_org_rl = sc.textFile(args(7)).filter(_.length!=0).filter(!_.contains("ORG"))
      .filter(line=>{
        val lineList = csvTokenUtil.split(line)
        var flag = true
        if(lineList.size() != 6){flag = false}
        else{
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty) {flag =false}
          }
          if(flag && !lineList.get(3).equals("C")){
            flag =false
          }
          if(flag && !lineList.get(4).startsWith("9999-12-31")){
            flag = false
          }
          val cim_mstr_slv_data = broadCIM_MSTER_SLV_RL.value
          val org_rl_tp_id = lineList.get(0)
          if(flag && !cim_mstr_slv_data.contains(org_rl_tp_id)){
            flag = false
          }
        }
        flag
      }).map(line=>{
      val lineList = csvTokenUtil.split(line)
      val SLV_ORG_ID = lineList.get(1)
      val MSTR_ORG_ID = lineList.get(2)
      (SLV_ORG_ID,MSTR_ORG_ID)
    }).collectAsMap()

    val broadMemberUniq2RootUniq = sc.broadcast(cim_org_rl)

    ///过滤交易维度表，以便更好的与交易记录表交易
    //找出 质押式回购中 失败的交易记录,用以删除bond_repo_deal表中，结算失败的记录

    val trdx_deal_infrmn_rmv_d_data = sc.textFile(args(8))
      .filter(_.length!=0).filter(!_.contains("dir")).filter(line=> {
      val lineList = csvTokenUtil.split(line)
      var flag = true
      if(lineList.size() != 5){
        flag =  false
      }else{
        for(i<-0 until lineList.size() if flag){
          if(lineList.get(i)==null || lineList.get(i).isEmpty){
            flag =false
          }
        }
        if(flag && !lineList.get(2).equals("109")){
          flag =false
        }
        if(flag){//过滤数据
          if(!lineList.get(0).equals("2") || !lineList.get(1).equals("1") ){
            flag = false //质押式失败交易集合
          }
        }
      }
      flag
    })
      .map(line=> {
        val lineList = csvTokenUtil.split(line)
        "11002"+lineList.get(3)
      }).collect().toSet

    val time8 = System.currentTimeMillis()
    println("load trdx_deal_infrmn_rmv_d cost "+ (time8-time7)/1000+" seconds and read "+trdx_deal_infrmn_rmv_d_data.size +" records")
    log.info("load trdx_deal_infrmn_rmv_d cost "+ (time8-time7)/1000+" seconds and read "+trdx_deal_infrmn_rmv_d_data.size +" records")

    val broadTRDX_DEAL_Fail_ID = sc.broadcast(trdx_deal_infrmn_rmv_d_data)

    //根据 a,d 2个条件进行过滤, 与broadTRDx相交， 过滤失败记录 .b条件待会使用，将ID映射为uniq
    val deal_cannot_find_ip_id = sc.longAccumulator("deal_cannot_find_ip_id") //无法从member_d表中找到机构id
    val bond_repo_deal_f_rdd = sc.textFile(args(9))
      .filter(_.length != 0).filter(!_.contains("EV"))
      .filter(line=>{
        val lineList = csvTokenUtil.split(line)
        var flag =true
        if(lineList.size() != 11){
          flag=false
        }else{
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty){
              flag =false
            }
          }
          if(flag && !lineList.get(9).equals("109")){ //得到质押式回购的记录,但过滤后的记录中，可能包含交易失败的，所以后面要继续过滤
            flag = false
          }
          if(flag && lineList.get(5).equals("2")) {flag= false}  //过滤撤销记录
          if(flag){
            val failed_trade_values = broadTRDX_DEAL_Fail_ID.value
            if(failed_trade_values.contains(lineList.get(0))) {
              flag = false ////与broadTRDx相交 若交易不在失败交易表里，则成功。若在则是失败的
            }
          }
        }
        flag
      })
      .map(line=>{     //得到正逆回购方Uniq
        val lineList = csvTokenUtil.split(line)
        val ev_id = lineList.get(0)
        val repo_id = lineList.get(1)
        val rvrse_id = lineList.get(2)
        var repo_uniq = "None"
        var rvrse_uniq = "None"
        val memberId2Uniq = broadMemberId2Uniq.value
        if(!memberId2Uniq.contains(repo_id)){
          log.warn("member_d doesnot contain member_id:"+repo_id)
        } else{
          repo_uniq = memberId2Uniq.get(repo_id).get
        }
        if(!memberId2Uniq.contains(rvrse_id)){
          log.warn("member_d doesnot contain member_id:"+rvrse_id)
        }else{
          rvrse_uniq = memberId2Uniq.get(rvrse_id).get
        }
        val trdng_pd_id =  lineList.get(3)
        val deal_nmbr = lineList.get(4)
     //   val trdng_ri_id = lineList.get(5)
        val st = lineList.get(5)
        val oracleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val deal_date = oracleDateFormat.parse(lineList.get(6))
        val deal_repo_rate =  BigDecimal.apply(lineList.get(7))
        val trade_amount =  BigDecimal.apply(lineList.get(8))
        (ev_id, (repo_uniq,rvrse_uniq,trdng_pd_id,deal_nmbr,st, deal_date,deal_repo_rate,trade_amount))//输出格式
      }).filter(record=>{
      var flag = true
      if(record._2._1.equals("None") || record._2._2.equals("None")){
        flag = false
      }
      flag
    }).cache()
    val time9 = System.currentTimeMillis()
    println("load bond_repo_deal_f cost "+ (time9-time8)/1000+" seconds")
    log.info("load bond_repo_deal_f cost "+ (time9-time8)/1000+" seconds")

    // println("file 5 bond_repo_deal_f read succeed! ")

    /*
    1、 清洗数据       步骤2)。Bond_d中bond_ctgry_nm进行变换。
    */
    val bond_d_data = sc.textFile(args(10))
      .filter(_.length!=0).filter(!_.contains("BOND"))
      .filter(line=>{
        val lineList = csvTokenUtil.split(line)
        var flag =true
        if(lineList.size() != 14){
          flag = false
        }
//        else {
//          for(i<-0 until lineList.size() if flag){
//            if(i== lineList.size()-1 && lineList.get(i)==null){
//                flag = false
//            }else{
//              if(lineList.get(i)==null || lineList.get(i).isEmpty){
//                flag =false
//              }
//            }
//          }
//        }
        flag
      })
      .map(line=>{
        val lineList = csvTokenUtil.split(line)
        val bond_id = lineList.get(0)
        val bond_prnt_tp_nm  = lineList.get(13) //换其它列
        var bond_ctgry_nm = "其它"
        if(bond_prnt_tp_nm.equals("利率债")){
          bond_ctgry_nm = "利率债"
        }else if(bond_prnt_tp_nm.equals("同业存单")){
          bond_ctgry_nm="同业存单"
        }else if(bond_prnt_tp_nm.equals("信用债")){
          bond_ctgry_nm = "信用债"
        }
       // val tmp  = lineList.get(2)
        var issr_id = lineList.get(2)
        if(issr_id ==null || issr_id.isEmpty){
          issr_id = ""
        }else if(issr_id.length>=4){
          issr_id = issr_id.substring(4,issr_id.length)
        }
        Tuple2(bond_id,(bond_ctgry_nm,issr_id))
      })
      .collectAsMap()

    val broadBondD = sc.broadcast(bond_d_data)
    val time10 = System.currentTimeMillis()
    log.info("load bond_d cost "+ (time10-time9)/1000+" seconds and read "+bond_d_data.size+" records")

    /*
   1、 清洗数据       步骤2)。sor.ri_credit_rtng中rtng_desc仅显示"AAA","AA+"和"AA及以下"。
   */
    val ri_credit_rtng = sc.textFile(args(11))
      .filter(_.length!=0).filter(!_.contains("RI"))
      .filter(line=>{
        val lineList = csvTokenUtil.split(line)
        var flag =true
        if(lineList.size() != 5){
          flag = false
        }else {
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty){
              flag =false
            }
          }
        }
        flag
      }).map(line=>{
      val lineList = csvTokenUtil.split(line)
      var rtng = ""
      if(lineList.get(1).equals("AAA")){
        rtng = "AAA"
      }else if(lineList.get(1).equals("AA+")||lineList.get(1).equals("A-1+")||lineList.get(1).equals("AA＋")){
        rtng = "AA+"
      }else{
        rtng = "AA及以下"
      }
      val oracleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val rtng_start_dt = oracleDateFormat.parse(lineList.get(2))
      val rtng_end_dt = oracleDateFormat.parse(lineList.get(3))
      val rtngRecord =Set(Tuple3(rtng_start_dt,rtng_end_dt,rtng))
      (lineList.get(0),rtngRecord)
    }).reduceByKey((x,y)=>{x++y})
      .mapValues(vSet=>{
        if(vSet.size>1){
          val rtngMap = Map("AA及以下"->0,"AA+"->1,"AAA"->2)
          val valueMap = Map(0->"AA及以下",1->"AA+",2->"AAA")
          val setArray = vSet.toArray
          val sortedSet = Set[Tuple3[Date,Date,String]]()
          for(i<-0 until setArray.length){
            var tmpRTNG = rtngMap.get(setArray(i)._3).get
            for(j<-0 until setArray.length){
              if(setArray(j)._1 == setArray(i)._1  && setArray(j)._2 == setArray(i)._2){
                val jValue  = rtngMap.get(setArray(j)._3).get
                if(jValue<tmpRTNG)
                  tmpRTNG = jValue
              }
            }
            sortedSet.add(Tuple3(setArray(i)._1,setArray(i)._2,valueMap.get(tmpRTNG).get))
          }
          sortedSet
        }else{
          vSet
        }
      })
      .collectAsMap()
    val broadRiCreditRtng = sc.broadcast(ri_credit_rtng)

    val time11 = System.currentTimeMillis()
    println("load ri_credit_rtng cost "+ (time11-time10)/1000+" seconds and read "+ri_credit_rtng.size+" records")
    log.info("load ri_credit_rtng cost "+ (time11-time10)/1000+" seconds and read "+ri_credit_rtng.size+" records")
    //用于找出银银间交易
    val dps_v_cr_dep_txn_dtl_data = sc.textFile(args(12))
      .filter(_.length!=0).filter(!_.contains("DL")).filter(line=>{
      val lineList = csvTokenUtil.split(line)
      var flag = true
      if(lineList.size() != 2){
        flag =false
      }else {
        for(i<-0 until lineList.size() if flag){
          if(lineList.get(i)==null || lineList.get(i).isEmpty){
            flag =false
          }
        }
        if(!lineList.get(1).equals("1")){
          flag = false
        }
      }
      flag
    })
      .map(line=>{
        val lineList = csvTokenUtil.split(line)
        lineList.get(0)
      }).collect.toSet

    val time12 = System.currentTimeMillis()
    println("load dps_v_cr_dep_txn_dtl cost "+ (time12-time11)/1000+" seconds and read "+dps_v_cr_dep_txn_dtl_data.size+" records")
    log.info("load dps_v_cr_dep_txn_dtl cost "+ (time12-time11)/1000+" seconds and read "+dps_v_cr_dep_txn_dtl_data.size+" records")

    if(dps_v_cr_dep_txn_dtl_data.size>2000000){
      log.warn("dps_v_Cr_dep_txn_Dtl size is:" +dps_v_cr_dep_txn_dtl_data.size)
    }
    val broadDpsDepTxnDtlData = sc.broadcast(dps_v_cr_dep_txn_dtl_data)
    println("file 9 dps_v_cr_dep_txn_dtl read succeed! read "+ dps_v_cr_dep_txn_dtl_data.size+" records")
    //!!!!数据清洗完毕
    /*
    算法2 对bond_repo_deal清洗后的数据进行映射得到 结果相关属性
      1.将trdng_pd_id 映射成 pd_cd(产品代码)
     2.将 trdnd_pri_id 映射成 rtng_desc

    3. 不需要ev_id和ST
    使用 机构ID，产品类型，日期 作为key
     */
    //将id替换为uniq编码,删除ST字段
   //   0          1       2             3        4       5      6          7              8
   // (ev_id, (repo_uniq,rvrse_uniq,trdng_pd_id,deal_nmbr,st, deal_date,deal_repo_rate,trade_amount))
    //给第7张表以后的数据要用（它们不区分PD_CD）
    val deal_cannot_find_trdngRiID = sc.longAccumulator("deal_cannot_find_trdngRiID")
    val deal_cannot_find_pdID  = sc.longAccumulator("deal_cannot_find_pdID")

    val bond_repo_deal_pd_filter_table= bond_repo_deal_f_rdd
      .map(x=>{
        val repo_pty_uniq = x._2._1
        val rvrse_repo_uniq = x._2._2
        val trdng_pd_id = x._2._3
        var pd_cd = "None"
        if(!broadPD_D.value.contains(trdng_pd_id)){
          deal_cannot_find_pdID.add(1l)
          log.warn("pd_d table does not contain this pd_id:"+trdng_pd_id)
        }else{
          pd_cd = broadPD_D.value.get(trdng_pd_id).get //根据trdng_pd_id得到 pd_cd
        }
        val deal_number = x._2._4
        val deal_dt = x._2._6
        val deal_repo_rate = x._2._7
        val trade_amnt = x._2._8
        (pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number, deal_repo_rate,trade_amnt, x._1)
      }
      ).filter(record=>{
      val pd_cd = record._1
      var flag  = false
      if(pd_cd.equals("R001") || pd_cd.equals("R007") || pd_cd.equals("R014")){
        flag = true
      }
      flag
    }).cache()

    /*
      机构 正回购 按  机构（ID），(汇总类型)，产品，日期 进行分组汇总
      这是 中间结果，这是因为父机构 需要包含子机构的结果，而且务必要保存子结构结果。
      保持这个中间结果的目的，是为计算  机构类型的结果做准备（父子机构类型可能不一致！！！）
// 1     2        3             4               5          6           7             8
pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,deal_repo_rate,trade_amnt, ev_id
    */
    val time13 = System.currentTimeMillis()
    val repo_pty_day = bond_repo_deal_pd_filter_table //按产品类型过滤后的表
      .map(x=>{Tuple2(Tuple3(x._3,x._1,x._2),Tuple2(x._6*x._7,x._7))}) //key ID，产品类型,日期
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2+y._2)).cache()
//输出: 1. repo_pty_uniq, 2.pd_cd, 3.  deal_dt

   val member_repo_day= repo_pty_day.map(record=>{
      val member_uniq = record._1._1
      val pd_cd  = record._1._2
      val deal_date = record._1._3
      var member_ins_show_nm = "None"
      if(!broadMemerUniq2Info.value.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemerUniq2Info.value.get(member_uniq).get._2
      }
      val repo_weight_price =  record._2._1/ record._2._2
      val repo_amount = record._2._2
     val key= Tuple4(member_uniq,member_ins_show_nm,pd_cd,deal_date)
     val value = Tuple2(repo_weight_price,repo_amount)
     (key,value)
    }).filter(record=>{!record._1._2.equals("None")})

    member_repo_day.map(record=>{
      val key = record._1
      val value = record._2
      val member_uniq = key._1
      val member_ins_show_nm = key._2
      val pd_cd  = key._3
      val deal_date= key._4
      val repo_weight_price =  value._1
      (member_uniq,pd_cd,deal_date,member_ins_show_nm,repo_weight_price.bigDecimal)
    }).saveToPhoenix("MEMBER_ALL_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS",
      "PD_CD","DEAL_DT","INS_SHOW_NAME", "REPO_WEIGHTED_PRICE"),conf,Some(ZOOKEEPER_URL))


    /*
    因为只需统计到父机构 各类型的 产品信息
     */
    val root_repo_pty_day = repo_pty_day
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)

        val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
        if(root_member_uniq.equals(member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"机构",record._1._2,record._1._3), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }else{
            arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2+y._2))
      .mapValues(x=>Tuple2(x._1/x._2,x._2)).cache()

    root_repo_pty_day  //将正回购结果写Hbase!!!
      .map(result=>{
      val deal_date = result._1._4
      (result._1._1,result._1._2,result._1._3,deal_date, result._2._1.bigDecimal,result._2._2.bigDecimal)
    })
      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "PD_CD","DEAL_DT", "REPO_WEIGHTED_PRICE","REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))
    val time14 = System.currentTimeMillis()
    println("机构正回购加权价统计耗时 "+(time14- time13)/1000+" seconds")
    log.info("机构正回购加权价统计耗时 "+(time14- time13)/1000+" seconds")

    /*
   // 机构 逆回购 按  机构，汇总类型，产品，日期 进行分组汇总
// 1     2        3             4               5          6           7             8
pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,deal_repo_rate,trade_amnt, ev_id
  */
    val rvrse_repo_pty_day = bond_repo_deal_pd_filter_table //按产品类型过滤后的表
      .map(x=>{Tuple2(Tuple3(x._4,x._1,x._2),Tuple2(x._6*x._7,x._7))}) //逆回购机构 ID，产品CD,日期
      .reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2)).cache()

//求所有机构的逆回购加权价
   val member_rvrse_day = rvrse_repo_pty_day.map(record=>{
      val member_uniq = record._1._1
      val pd_cd  = record._1._2
      val deal_date = record._1._3
      var member_ins_show_nm = "None"
      if(!broadMemerUniq2Info.value.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemerUniq2Info.value.get(member_uniq).get._2
      }
      val rvrse_weight_price =  record._2._1/ record._2._2
      val rvrse_amount = record._2._2
     val key= Tuple4(member_uniq,member_ins_show_nm,pd_cd,deal_date)
     val value = Tuple2(rvrse_weight_price,rvrse_amount)
     (key,value)
    }).filter(record=>{!record._1._2.equals("None")})

    member_rvrse_day.map(record=>{
      val key = record._1
      val value = record._2
      val member_uniq = key._1
      val member_ins_show_nm = key._2
      val pd_cd  = key._3
      val deal_date= key._4
      val rvrse_weight_price =  value._1
      (member_uniq,pd_cd,deal_date,member_ins_show_nm,rvrse_weight_price.bigDecimal)
    }).saveToPhoenix("MEMBER_ALL_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS",
      "PD_CD","DEAL_DT","INS_SHOW_NAME", "RVRSE_WEIGHTED_PRICE"),conf,Some(ZOOKEEPER_URL))

//    /*
//算每个机构的价差和总量
// */
//    val mem_price_diffrence = member_repo_day.join(member_rvrse_day)
//      .mapValues(record=>{
//        val repo = record._1
//        val rvrse = record._2
//        Tuple2 (rvrse._1 - repo._1, repo._2+ rvrse._2)//加权价(逆-正)相减 得到价差，amount相加得 到总amount
//      })
//
//      mem_price_diffrence.map(result=>{ //key : member_uniq,pd_cd,deal_date,member_ins_show_nm
//      (result._1._1,result._1._2,result._1._3,result._1._4, result._2._1.bigDecimal,result._2._2.bigDecimal)
//       })
//      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS", "PD_CD",
//        "DEAL_DT","INS_SHOW_NAME", "PRICE_DIFFERENCE","TOTAL_AMOUNT"),conf,Some(ZOOKEEPER_URL))
// //?????????? 机构类型价差


    /*
    因为只需统计到父机构 各类型的 产品信息
   */
    val root_rvrse_repo_pty_day = rvrse_repo_pty_day
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq  = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"机构",record._1._2,record._1._3), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }else{
            arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      })
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2+y._2)) //将结果写HBase!!
      .mapValues(x=>Tuple2(x._1/x._2,x._2)).cache() // 结果为<sum(加权价)/sum(amount),sum(amount)>

    root_rvrse_repo_pty_day //将逆回购结果写Hbase!!!
      .map(result=>{
      val deal_date = result._1._4
      (result._1._1,result._1._2,result._1._3,deal_date, result._2._1.bigDecimal,result._2._2.bigDecimal)
    })
      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "PD_CD","DEAL_DT", "RVRSE_WEIGHTED_PRICE","RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))
    val time15 = System.currentTimeMillis()
    println("机构逆回购加权价统计耗时 "+(time15- time14)/1000+" seconds")
    log.info("机构逆回购加权价统计耗时 "+(time15- time14)/1000+" seconds")

    /*
      正逆回购间价差表
    */
    root_repo_pty_day.join(root_rvrse_repo_pty_day) //!!!!将结果写Hbase.
      .mapValues(x=>{
      val repo = x._1
      val rvrse = x._2
      Tuple2 (rvrse._1 - repo._1, repo._2+ rvrse._2)//加权价(逆-正)相减 得到价差，amount相加得 到总amount
    })
      .map(result=>{
        (result._1._1,result._1._2,result._1._3,result._1._4, result._2._1.bigDecimal,result._2._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "PD_CD","DEAL_DT", "PRICE_DIFFERENCE","TOTAL_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    /*
       机构类型(展示类型，非机构真正的类型)   正回购 统计 （对汇总结果按类型汇总!）
      */
    val ctgry_repo_pty_day = repo_pty_day.map(record=>{
      val member_uniq = record._1._1 //得到机构ID
      var member_ins_show_nm = "None"
      if(!broadMemerUniq2Info.value.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemerUniq2Info.value.get(member_uniq).get._2
      }      //key: 机构类型，汇总类型，产品，日期
      (Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2))
      .filter(x=>{!x._1._1.equals("None")})
      .mapValues(x=>x._1/x._2).cache() //sum(加权价)/sum(amount) ,无需保持总量



    //结果 正回购价写表
    ctgry_repo_pty_day.map(result=>{
      val deal_date = result._1._3
      (result._1._1,result._1._2,deal_date,result._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "REPO_WEIGHTED_PRICE"),conf,Some(ZOOKEEPER_URL))
    val time16 = System.currentTimeMillis()
    println("机构类型正回购加权价统计耗时 "+(time16- time15)/1000+" seconds")
    log.info("机构类型正回购加权价统计耗时 "+(time16- time15)/1000+" seconds")

    /*
       机构类型(展示类型，非机构真正的类型) 逆回购 统计 （对汇总结果按类型汇总!）
      */
    val ctgry_rvrse_repo_pty_day = rvrse_repo_pty_day.map(record=>{
      val member_uniq = record._1._1 //得到机构uiq
      var member_ins_show_nm = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
      }
      //key: 机构类型，汇总类型，产品，日期
      (Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2))
      .filter(x=>{!x._1._1.equals("None")})
      .mapValues(x=>x._1/x._2).cache() //sum(加权价)/sum(amount)

    //结果 逆回购价写表
    ctgry_rvrse_repo_pty_day.map(result=>{
      val deal_date = result._1._3
      (result._1._1,result._1._2,deal_date,result._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "RVRSE_WEIGHTED_PRICE"),conf,Some(ZOOKEEPER_URL))
    val time17 = System.currentTimeMillis()
    println("机构类型逆回购加权价统计耗时 "+(time17- time16)/1000+" seconds")
    log.info("机构类型逆回购加权价统计耗时 "+(time17- time16)/1000+" seconds")

 member_repo_day
  .join(member_rvrse_day)
  .mapValues(x=>{
    val repo = x._1
    val rvrse = x._2
    val price_diff = rvrse._1 - repo._1
    val total_amount  =  rvrse._2 + repo._2
    (price_diff*total_amount ,total_amount) //得到机构的价差 和amount
  })
  .map(record=>{
    val ins_show_name  = record._1._2
    val pd_cd = record._1._3
    val deal_date = record._1._4
    (Tuple3(ins_show_name,pd_cd,deal_date),record._2)
  }).reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2))
  .mapValues(x=>x._1/x._2)
  .map(record=>{
    (record._1._1,record._1._2,record._1._3,record._2.bigDecimal)
  })
  .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
    "PRICE_DIFFERENCE"),conf,Some(ZOOKEEPER_URL))
    val time18 = System.currentTimeMillis()
    println("机构类型正逆回购价差统计耗时 "+(time18- time17)/1000+" seconds")
    log.info("机构类型正逆回购价差统计耗时 "+(time18- time17)/1000+" seconds")

    //正回购 按类型 单条记录汇总(类型,产品,日期)
    // 1     2        3             4               5          6           7             8
 // pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,deal_repo_rate,trade_amnt, ev_id
     val ctgryRepoRecords = bond_repo_deal_pd_filter_table.map(x=>{
      val member_uniq = x._3
      var member_ins_show_nm = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
      }
      Tuple2(Tuple3(member_ins_show_nm,x._1,x._2),x._6)})
      .filter(x=>{!x._1._1.equals("None")})
      .cache()

    //正回购 按类型最大 写表
    ctgryRepoRecords.reduceByKey((x,y)=>{
      if(x>y){
        x
      }else{
        y
      }
    }).map(x=>{
      val deal_date = x._1._3
      (x._1._1,x._1._2,deal_date,x._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "REPO_MAX_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    //正回购 按类型最小  写Hbase
    ctgryRepoRecords.reduceByKey((x,y)=>{
      if(x<y){
        x
      }else{
        y
      }
    }).map(x=>{
      val deal_date = x._1._3
      (x._1._1,x._1._2,deal_date,x._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "REPO_MIN_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    val time19 = System.currentTimeMillis()
    println("机构类型正回购最大最小repo_rate 统计耗时 "+(time19- time18)/1000+" seconds")
    log.info("机构类型正回购最大最小repo_rate 统计耗时 "+(time19- time18)/1000+" seconds")

    //逆回购 单条交易汇总
      // 1     2        3             4               5          6           7             8
    // pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,deal_repo_rate,trade_amnt, ev_id
    val ctgryRvrseRepoRecords = bond_repo_deal_pd_filter_table.map(x=>{
      val member_uniq = x._4
      var member_ins_show_nm = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
      }
      Tuple2(Tuple3(member_ins_show_nm,x._1,x._2),x._6)})
        .filter(x=>{!x._1._1.equals("None")})
        .cache()

    //逆回购 类型最大 写Hbase
    ctgryRvrseRepoRecords.reduceByKey((x,y)=>{
      if(x>y){
        x
      }else{
        y
      }
    }).map(x=>{
      val deal_date = x._1._3
      (x._1._1,x._1._2,deal_date,x._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "RVRSE_MAX_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))
    //逆回购 类型最小 写Hbase
    ctgryRvrseRepoRecords.reduceByKey((x,y)=>{
      if(x<y){
        x
      }else{
        y
      }
    }).map(x=>{
      val deal_date = x._1._3
      (x._1._1,x._1._2,deal_date,x._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "RVRSE_MIN_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    val time20 = System.currentTimeMillis()
    println("机构类型逆回购最大最小repo_rate 统计耗时 "+(time20- time19)/1000+" seconds")
    log.info("机构类型逆回购最大最小repo_rate 统计耗时 "+(time20- time19)/1000+" seconds")

    //       1     2        3             4               5          6             7             8
    // // pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,deal_repo_rate,trade_amnt, ev_id
    //全市场 每个产品 每天 加权价，
    bond_repo_deal_pd_filter_table.map(record=>{
      val pd_cd = record._1
      val deal_dt = record._2
      val repo_rate = record._6
      val amount = record._7
      Tuple2 (Tuple2(pd_cd,deal_dt), Tuple2(repo_rate*amount,amount))
    }).reduceByKey((x,y)=>{
      (x._1 + y._1 ,x._2+y._2)
    }).mapValues(x=>x._1/x._2)
      .map(x=>{
        val deal_date = x._1._2
        (x._1._1,deal_date ,x._2.bigDecimal)})
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT", "MARKET_WEIGHTED_PRICE"), conf, Some(ZOOKEEPER_URL))

    // 求最大和最小
    // 1     2        3             4               5          6           7             8
    // pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,deal_repo_rate,trade_amnt, ev_id
    val market_repo_rates = bond_repo_deal_pd_filter_table.map(record=>{
      val pd_cd = record._1
      val deal_dt = record._2
      val repo_rate = record._6
      Tuple2 (Tuple2(pd_cd,deal_dt), repo_rate)
    }).cache
    //全市场 最大 和最小 repo_rate 求PD_CD, DEAl_DT
    market_repo_rates.reduceByKey((x,y)=>{if(x>y)x else y})
      .map(x=>{
        val deal_date = x._1._2
        (x._1._1,deal_date ,x._2.bigDecimal)}) //最大
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "MARKET_MAX_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    market_repo_rates.reduceByKey((x,y)=>{if(x<y)x else y})
      .map(x=>{
        val deal_date = x._1._2
        (x._1._1,deal_date ,x._2.bigDecimal)}) //最小
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "MARKET_MIN_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    val time21 = System.currentTimeMillis()
    println("全市场信息统计耗时 "+(time21- time20)/1000+" seconds")
    log.info("全市场信息统计耗时 "+(time21- time20)/1000+" seconds")

    //银银间 正逆 回购 加权价，最大 和最小 repo_rate
    //首先过滤数据
    // 1     2        3             4               5             6           7             8
    // pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,deal_repo_rate,trade_amnt, ev_id
    val banks_records = bond_repo_deal_pd_filter_table
      .filter(record=>{
        val deal_number = record._5//成交编号
        var flag = true
        if(!broadDpsDepTxnDtlData.value.contains(deal_number)){
          flag = false
        }
        flag
      })
      .cache()
      // 1     2        3             4               5             6           7             8
    // pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,deal_repo_rate,trade_amnt, ev_id
    banks_records.map(record=>{
      val pd_cd = record._1
      val deal_dt = record._2
      val repo_rate = record._6
      val amount = record._7
      Tuple2 (Tuple2(pd_cd,deal_dt), Tuple2(repo_rate * amount,amount))
    }).reduceByKey((x,y)=>{
      (x._1 + y._1,x._2+y._2)
    }).mapValues(x=>x._1/x._2)
      .map(x=>{
        val deal_date = x._1._2
        (x._1._1,deal_date ,x._2.bigDecimal)})
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
        "BANKS_WEIGHTED_PRICE"),conf,Some(ZOOKEEPER_URL))

    //统计银银间 最大和最小 先将repo_rate统计出来
    val banks_repo_rates = banks_records.map(record=>{
      val pd_cd = record._1
      val deal_dt = record._2
      val repo_rate = record._6
      Tuple2 (Tuple2(pd_cd,deal_dt), repo_rate)
    }).cache
    //银银间最大 和最小 repo_rate 求PD_CD, DEAl_DT
    banks_repo_rates.reduceByKey((x,y)=>{if(x>y)x else y})
      .map(x=>{
        val deal_date = x._1._2
        (x._1._1,deal_date ,x._2.bigDecimal)}) //最大
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "BANKS_MAX_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    banks_repo_rates.reduceByKey((x,y)=>{if(x<y)x else y})
      .map(x=>{
        val deal_date = x._1._2
        (x._1._1,deal_date ,x._2.bigDecimal)}) //最小
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "BANKS_MIN_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    val time22 = System.currentTimeMillis()
    println("银银间信息统计耗时 "+(time22- time21)/1000+" seconds")
    log.info("银银间信息统计耗时 "+(time22- time21)/1000+" seconds")



    /*
    机构交易 分布图例
     原始输入
    //   0          1       2             3        4       5      6          7              8
   // (ev_id, (repo_uniq,rvrse_uniq,trdng_pd_id,deal_nmbr,st, deal_date,deal_repo_rate,trade_amount))
     结果 输出数据。（根）机构ID，参与者类型,对比机构类型,日期,交易金额
     */
    //考虑正回购信息
    val repo_pty_distribution_day= bond_repo_deal_f_rdd.map(x=>{
      val member_uniq = x._2._1
      val rvrMember_uniq = x._2._2
      val deal_date = x._2._6
      val trade_amount = x._2._8
      var rvrInsShowName = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(rvrMember_uniq)){
        log.warn("broadMemberUniq2Name does not contain: member uniq:"+rvrMember_uniq)
      }else{
        rvrInsShowName = memberUniq2Info.get(rvrMember_uniq).get._2
      }
      //key:          机构 unqID，  对手机构类型，  日期;    value：amount
      Tuple2(Tuple3(member_uniq,rvrInsShowName,deal_date),trade_amount)
    }).filter(x=>{!x._1._2.equals("None")})
      .cache()
    //正回购 按机构进行统计，结果写Hbase!
    repo_pty_distribution_day.flatMap(record=>{
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"机构",record._1._2,record._1._3), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }else{
            arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x + y}) //key: ID,参与者类型,对比机构类型,日期 value:amount
      .map(record=>{
      val deal_date = record._1._4
      (record._1._1,record._1._2,record._1._3,deal_date,record._2.bigDecimal)})
      .saveToPhoenix("MEMBER_CTGRY_AMOUNT_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "COU_PARTY_INS_SHOW_NAME","DEAL_DT","REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    val time23 = System.currentTimeMillis()
    println("MEMBER_CTGRY_AMOUNT_DEAL_INFO 表 正回购统计耗时 "+(time23 - time22)/1000+" seconds")
    log.info("MEMBER_CTGRY_AMOUNT_DEAL_INFO 表 正回购统计耗时 "+(time23 - time22)/1000+" seconds")

    //正回购 按机构类型统计，结果写Hbase!
    repo_pty_distribution_day
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
        }
        Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
      })
      .filter(x=>{!x._1._1.equals("None")})
      .reduceByKey((x,y)=>{x+y}) //
      .map(record=>{
      val deal_date = record._1._3
      (record._1._1,record._1._2,deal_date,record._2.bigDecimal)})
      .saveToPhoenix("CTGRY_CTGRY_AMOUNT_DEAL_INFO",Seq("INS_SHOW_NAME", "COU_PARTY_INS_SHOW_NAME",
        "DEAL_DT","REPO_SUM_AMOUNT"), conf, Some(ZOOKEEPER_URL))
    val time24 = System.currentTimeMillis()
    println("CTGRY_CTGRY_AMOUNT_DEAL_INFO 表 正回购统计耗时 "+(time24 - time23)/1000+" seconds")
    log.info("CTGRY_CTGRY_AMOUNT_DEAL_INFO 表 正回购统计耗时 "+(time24 - time23)/1000+" seconds")

    //考虑逆回购 信息
//   0          1       2             3        4       5      6          7              8
// (ev_id, (repo_uniq,rvrse_uniq,trdng_pd_id,deal_nmbr,st, deal_date,deal_repo_rate,trade_amount))


    val rvrse_repo_pty_distribution_day = bond_repo_deal_f_rdd.map(record=>{
      val deal_date = record._2._6
      val member_uniq = record._2._1 //正回购方
      val rvrMember_uniq = record._2._2 //逆回购方
      val trade_amount = record._2._8
      var memInsShowName = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain: member uniq:"+member_uniq)
      }else{
        memInsShowName = memberUniq2Info.get(member_uniq).get._2
      }
      //key:          机构ID，  对手机构，          日期；    value：amount
      Tuple2(Tuple3(rvrMember_uniq,memInsShowName,deal_date),trade_amount)
    }).filter(x=>{!x._1._2.equals("None")})
      .cache()

    //逆回购 按机构统计
    rvrse_repo_pty_distribution_day.flatMap(record=>{
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"机构",record._1._2,record._1._3), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }else{
            arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y}) //key: ID,参与者类型,对比机构类型,日期 value:amount
      .map(record=>{
      val deal_date = record._1._4
      (record._1._1,record._1._2,record._1._3,deal_date,record._2.bigDecimal)
    })
      .saveToPhoenix("MEMBER_CTGRY_AMOUNT_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "COU_PARTY_INS_SHOW_NAME","DEAL_DT","RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))
    val time25 = System.currentTimeMillis()
    println("MEMBER_CTGRY_AMOUNT_DEAL_INFO 表 逆回购统计耗时 "+(time25- time24)/1000+" seconds")
    log.info("MEMBER_CTGRY_AMOUNT_DEAL_INFO 表 逆回购统计耗时 "+(time25- time24)/1000+" seconds")

    //逆回购 按机构类型统计，结果写Hbase!
    rvrse_repo_pty_distribution_day
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
        }
        Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
      }).filter(x=>{!x._1._1.equals("None")})
      .reduceByKey((x,y)=>{x+y})
      .map(record=>{
        val deal_date = record._1._3
        (record._1._1,record._1._2,deal_date,record._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_CTGRY_AMOUNT_DEAL_INFO",Seq("INS_SHOW_NAME", "COU_PARTY_INS_SHOW_NAME", "DEAL_DT","RVRSE_SUM_AMOUNT"), conf, Some(ZOOKEEPER_URL))
    val time26 = System.currentTimeMillis()
    println("CTGRY_CTGRY_AMOUNT_DEAL_INFO 表 逆回购统计耗时 "+(time26- time25)/1000+" seconds")
    log.info("CTGRY_CTGRY_AMOUNT_DEAL_INFO 表 逆回购统计耗时 "+(time26- time25)/1000+" seconds")


    /*
        分布图 考虑在 各机构类型（机构数）上的分布
//   0          1       2             3        4       5      6          7              8
// (ev_id, (repo_uniq,rvrse_uniq,trdng_pd_id,deal_nmbr,st, deal_date,deal_repo_rate,trade_amount))
     */
    //考虑 机构 正回购信息

    val repo_pty_jigou_distribution_day =  bond_repo_deal_f_rdd.map(record=>{
      val deal_date = record._2._6
      val member_uniq = record._2._1 //正回购方
      val rvrMember_uniq = record._2._2 //逆回购方
      var rvrInsShowName = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(rvrMember_uniq)){
        log.warn("memberUniq2Info does not contain: member uniq:"+rvrMember_uniq)
      }else{
        rvrInsShowName = memberUniq2Info.get(rvrMember_uniq).get._2
      }
      val rvrSet = Set(rvrMember_uniq)
      Tuple2(Tuple3(member_uniq,rvrInsShowName,deal_date),rvrSet)
    }).filter(x=>{!x._1._2.equals("None")})
      .cache()

    repo_pty_jigou_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq   = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],Set[String])]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4(root_member_uniq,"机构",record._1._2,record._1._3), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }else{
            arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>{x++y}) //得到 每个机构的结果集
      .map(record=>{
      val member_uniq =  record._1._1
      val join_type = record._1._2
      val rvrInsShowName = record._1._3
      val memberSetString = record._2.mkString(",")
      val deal_date = record._1._4
      (member_uniq,join_type,rvrInsShowName,deal_date, memberSetString)
    }).saveToPhoenix("MEMBER_CTGRY_AMOUNT_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE", "COU_PARTY_INS_SHOW_NAME","DEAL_DT", "REPO_COU_PARTY_UNIQ_SET"), conf, Some(ZOOKEEPER_URL))
    val time27 = System.currentTimeMillis()
    println("MEMBER_CTGRY_AMOUNT_DEAL_INFO 表 正回购统计耗时 "+(time27- time26)/1000+" seconds")
    log.info("MEMBER_CTGRY_AMOUNT_DEAL_INFO 表 正回购统计耗时 "+(time27- time26)/1000+" seconds")

    repo_pty_jigou_distribution_day.map(record=>{
        val member_uniq =  record._1._1
        var member_ins_show_nm = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
        }
        Tuple2(Tuple4(member_ins_show_nm,member_uniq,record._1._2,record._1._3),record._2)
      }).filter(x=>{!x._1._1.equals("None")})
      .reduceByKey((x,y)=>{x++y})
      .map(record=>{
        val memInsShowName = record._1._1
        val mem_uniq = record._1._2
        val rvrInsShowName = record._1._3
        val deal_date = record._1._4
        val memberSetString = record._2.mkString(",")
        (memInsShowName,mem_uniq,rvrInsShowName,deal_date,memberSetString)
      }).saveToPhoenix("MEMBER_CTGRY_NUMBER_DEAL_INFO",Seq("INS_SHOW_NAME","UNQ_ID_IN_SRC_SYS",
      "COU_PARTY_INS_SHOW_NAME","DEAL_DT","REPO_COU_PARTY_UNIQ_SET"),conf,Some(ZOOKEEPER_URL))
    val time28 = System.currentTimeMillis()
    println("MEMBER_CTGRY_NUMBER_DEAL_INFO 表按展示名 正回购统计耗时 "+(time28- time27)/1000+" seconds")
    log.info("MEMBER_CTGRY_NUMBER_DEAL_INFO 表按展示名 正回购统计耗时 "+(time28- time27)/1000+" seconds")

    //   0          1       2             3        4       5      6          7              8
    // (ev_id, (repo_uniq,rvrse_uniq,trdng_pd_id,deal_nmbr,st, deal_date,deal_repo_rate,trade_amount))
    //考虑 机构 逆回购信息
    val rvrse_repo_pty_jigou_distribution_day = bond_repo_deal_f_rdd.map(record=>{
      val deal_date = record._2._6
      val member_uniq = record._2._1 //正回购方
      val rvrMember_uniq = record._2._2 //逆回购方
      var memInsShowName = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){ //此时对手机构 为正回购
        log.warn("broadMemberUniq2Name does not contain: member uniq:"+member_uniq)
      }else{
        memInsShowName = memberUniq2Info.get(member_uniq).get._2
      }
      val memSet = Set(member_uniq)
      Tuple2(Tuple3(rvrMember_uniq,memInsShowName,deal_date),memSet)
    }).filter(x=>{!x._1._2.equals("None")})
      .cache()

    //逆回购 对手机构集合统计
    rvrse_repo_pty_jigou_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq   = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],Set[String])]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4(root_member_uniq,"机构",record._1._2,record._1._3), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }else{
            arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>{x++y}) //得到 每个机构的结果集
      .map(record=>{
      val member_uniq =  record._1._1
      val memberSetString = record._2.mkString(",")
      val deal_date = record._1._4
      (member_uniq,record._1._2,record._1._3,deal_date, memberSetString)
    }).saveToPhoenix("MEMBER_CTGRY_AMOUNT_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
      "COU_PARTY_INS_SHOW_NAME","DEAL_DT","RVRSE_COU_PARTY_UNIQ_SET"),conf,Some(ZOOKEEPER_URL))
    val time29 = System.currentTimeMillis()
    println("MEMBER_CTGRY_AMOUNT_DEAL_INFO 表按展示名 正回购统计耗时 "+(time29- time28)/1000+" seconds")
    log.info("MEMBER_CTGRY_AMOUNT_DEAL_INFO 表按展示名 正回购统计耗时 "+(time29- time28)/1000+" seconds")

    //
    rvrse_repo_pty_jigou_distribution_day
      .map(record=>{
        val member_uniq =  record._1._1 //逆回购方id
        var member_ins_show_nm = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
        }
        Tuple2(Tuple4(member_ins_show_nm,member_uniq,record._1._2,record._1._3),record._2)
      }).filter(x=>{!x._1._1.equals("None")})
      .reduceByKey((x,y)=>{x++y})
      .map(record=>{
        val memInsShowName = record._1._1
        val mem_uniq = record._1._2
        val rvrInsShowName = record._1._3 //交易对手类型
        val deal_date = record._1._4
        val memberSetString = record._2.mkString(",")
        (memInsShowName,mem_uniq,rvrInsShowName,deal_date,memberSetString)
      }).saveToPhoenix("MEMBER_CTGRY_NUMBER_DEAL_INFO",Seq("INS_SHOW_NAME","UNQ_ID_IN_SRC_SYS",
      "COU_PARTY_INS_SHOW_NAME","DEAL_DT","RVRSE_COU_PARTY_UNIQ_SET"),conf,Some(ZOOKEEPER_URL))
    val time30 = System.currentTimeMillis()
    println("MEMBER_CTGRY_NUMBER_DEAL_INFO 表按类型 逆回购统计耗时 "+(time30- time29)/1000+" seconds")
    log.info("MEMBER_CTGRY_NUMBER_DEAL_INFO 表按类型 逆回购统计耗时 "+(time30- time29)/1000+" seconds")



    //得到一笔交易的bond_id,首先将 ev_cltrl_dtls的 ri_id 与bond_d的 bond_id 相join
    //1.对 ev_cltrl_dtls进行预处理得到 (ev_id,Set(bond_id,cnvrsn_prprtn))
    val ev_cltrl_dtls = sc.textFile(args(13))
      .filter(_.length!=0).filter(!_.contains("RI"))
      .filter(line=>{
        val lineList = csvTokenUtil.split(line)
        var flag = true
        if(lineList.size() != 5){
          flag = false
        }else {
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty ){flag =false}
          }
        }
        flag
      }).map(line=>{
      val lineList = csvTokenUtil.split(line)
      val ri_id = lineList.get(0)
      val ev_id = lineList.get(1)
      val cnvrsn_prprtn = BigDecimal.apply(lineList.get(2))
      val ttl_face_value = BigDecimal.apply(lineList.get(3))

      val bond_deals = Set(Tuple3(ri_id,cnvrsn_prprtn,ttl_face_value))
      (ev_id,bond_deals)
    }).reduceByKey((x,y)=>{x++y}) //得到每笔交易在各个债券上的分布
      .mapValues(values=>{
      var sum= BigDecimal.apply(0)
      for(value<-values){
          sum =  sum + value._3
      }
      val ratio_bond_deal_array = new ArrayBuffer[(String,BigDecimal,BigDecimal)]()
      for(value<-values){
        try{
          if(sum<BigDecimal.apply(0.000000001)){
            ratio_bond_deal_array += Tuple3(value._1,value._2,value._3)
          }else{
            ratio_bond_deal_array += Tuple3(value._1,value._2,value._3/sum)
          }
        }catch {
          case ex:Exception =>{println(values)}
        }
      }
      ratio_bond_deal_array.toArray
    })

    val time31 = System.currentTimeMillis()
    println("读取ev_cltrl_dtls 耗时 "+(time31- time30)/1000+" seconds")
    log.info("读取ev_cltrl_dtls 耗时 "+(time31- time30)/1000+" seconds")


    //   0          1       2             3        4       5      6          7              8
    // (ev_id, (repo_uniq,rvrse_uniq,trdng_pd_id,deal_nmbr,st, deal_date,deal_repo_rate,trade_amount))

    val repo_ev_joined_dtls = bond_repo_deal_f_rdd.map(x=>{
      val ev_id =x._1
      val repo_pty_uniq = x._2._1
      val rvrse_repo_uniq = x._2._2
      val deal_dt = x._2._6
      val deal_repo_rate = x._2._7
      val trade_amount = x._2._8
      (ev_id,(deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_repo_rate, trade_amount))
    }).join(ev_cltrl_dtls)
      .map(x=>{
        val deal_dt = x._2._1._1
        val repo_pty_uniq = x._2._1._2
        val rvrse_repo_uniq = x._2._1._3
        val deal_repo_rate = x._2._1._4
        val trade_amount =x._2._1._5
        val bond_deal_info = x._2._2
        (deal_dt,repo_pty_uniq,rvrse_repo_uniq, deal_repo_rate,trade_amount,bond_deal_info)
      }).cache()

    val time32 = System.currentTimeMillis()
    println("ev_cltrl_dtls 与 bond_repo_deal_f join耗时 "+(time32- time31)/1000+" seconds")
    log.info("ev_cltrl_dtls 与 bond_repo_deal_f join耗时 "+(time32- time31)/1000+" seconds")
    /*
      正逆回购 在各质押物类型上的分布 root机构 参与者类型 质押物类型，每天，amount累加
      需要统计所有产品上的信息，所以用 bond_repo_deal_table
     */
    //  1        2               3                4              5           6
 //   (deal_dt,repo_pty_uniq,rvrse_repo_uniq, deal_repo_rate,trade_amount,bond_deal_info)
    //正回购机构，质押物类型，每天，amount
    val repo_zhiyawuleixing_rdd = repo_ev_joined_dtls.flatMap(record=>{
      val member_uniq = record._2 //正回购方
      val deal_date = record._1
      val trade_amount = record._5
      val bond_Infos = record._6
      val arrays = new ArrayBuffer[(Tuple3[String,String,Date],BigDecimal)]()
      for(info<- bond_Infos){
          val bond_id = info._1
          val ratio = info._3
          var bond_ctgry_nm = "其他"
          if(broadBondD.value.contains(bond_id)){
            val bondInfo = broadBondD.value.get(bond_id).get
            bond_ctgry_nm = bondInfo._1
          }
        arrays += Tuple2 (Tuple3(member_uniq,bond_ctgry_nm,deal_date),ratio*trade_amount)
      }
      arrays.iterator  //机构ID，质押物类型，日期，amount
    }).cache()

    //正 root机构，参与者类型，质押物类型，每天，amount
    //Tuple2 (Tuple3(member_uniq,bond_ctgry_nm,deal_date),ratio*trade_amount)
    repo_zhiyawuleixing_rdd.flatMap(record=>{
      val member_uniq = record._1._1
      val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
      val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],BigDecimal)]()
      arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
      if(member_uniq.equals(root_member_uniq)){
        arrays += Tuple2(Tuple4 (root_member_uniq,"机构",record._1._2,record._1._3), record._2)
      }else{
        var member_full_name = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
        }else{
          member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
        }
        if(member_full_name.contains("货币市场基金")){
          arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
        }
      }
      arrays.iterator
    }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val deal_date = result._1._4
        (result._1._1,result._1._2,result._1._3,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_CTGRY_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","DEAL_DT", "REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    val time33 = System.currentTimeMillis()
    println("MEMBER_BOND_CTGRY_DEAL_INFO表 正回购统计耗时 "+(time33- time32)/1000+" seconds")
    log.info("MEMBER_BOND_CTGRY_DEAL_INFO表 正回购统计耗时 "+(time33- time32)/1000+" seconds")


    repo_zhiyawuleixing_rdd.map(record=>{
      val member_uniq = record._1._1
      var member_ins_show_nm = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
      }
      Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>{x+y})
      .filter(x=>{!x._1._1.equals("None")})
      .map(result=>{
        val deal_date = result._1._3
        (result._1._1,result._1._2,deal_date, result._2.bigDecimal)
              // (result._1._1,result._1._2,result._1._3, result._2)
      })
      .saveToPhoenix("CTGRY_BOND_CTGRY_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM","DEAL_DT", "REPO_SUM_AMOUNT"), conf, Some(ZOOKEEPER_URL))
    val time34 = System.currentTimeMillis()
    println("CTGRY_BOND_CTGRY_DEAL_INFO 表 正回购统计耗时 "+(time34- time33)/1000+" seconds")
    log.info("CTGRY_BOND_CTGRY_DEAL_INFO 表 正回购统计耗时 "+(time34- time33)/1000+" seconds")

    /*
    统计逆回购的相关信息
    输入:(deal_dt,repo_pty_uniq,rvrse_repo_uniq, deal_repo_rate,trade_amount,bond_deal_info)
     */
    val rvrse_repo_zhiyawuleixing_rdd = repo_ev_joined_dtls.flatMap(record=>{
      val member_uniq = record._3 //逆回购方
      val deal_date = record._1
      val trade_amount = record._5
      val bond_Infos = record._6
      val arrays = new ArrayBuffer[(Tuple3[String,String,Date],BigDecimal)]()
      for(info<- bond_Infos){
        val bond_id = info._1
        val ratio = info._3
        var bond_ctgry_nm = "其他"
        if(broadBondD.value.contains(bond_id)){
          val bondInfo = broadBondD.value.get(bond_id).get
          bond_ctgry_nm = bondInfo._1
        }
        arrays += Tuple2 (Tuple3(member_uniq,bond_ctgry_nm,deal_date),ratio*trade_amount)
      }
      arrays.iterator  //机构ID，质押物类型，日期，amount
    }).cache()

    //统计 根机构信息,得到每个机构的在每个分布上的amout之和     结果写HBase！！！！！
    //输入：Tuple2 (Tuple3(member_uniq,bond_ctgry_nm,deal_date),ratio*trade_amount)
    rvrse_repo_zhiyawuleixing_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"机构",record._1._2,record._1._3), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }else{
            arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val deal_date = result._1._4
        (result._1._1,result._1._2,result._1._3,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_CTGRY_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","DEAL_DT", "RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    val time35 = System.currentTimeMillis()
    println("MEMBER_BOND_CTGRY_DEAL_INFO 表 逆回购统计耗时 "+(time35 - time34)/1000+" seconds")
    log.info("MEMBER_BOND_CTGRY_DEAL_INFO 表 逆回购统计耗时 "+(time35 - time34)/1000+" seconds")

    //统计 机构类型信息,得到每个机构的在每个分布上的amout之和    结果写HBase!!!!!!
    rvrse_repo_zhiyawuleixing_rdd
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
        }
        Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>{x+y})
      .filter(x=>{!x._1._1.equals("None")})
      .map(result=>{
        val deal_date = result._1._3
        (result._1._1,result._1._2,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_BOND_CTGRY_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM","DEAL_DT",
        "RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))
    val time36 = System.currentTimeMillis()

    println("CTGRY_BOND_CTGRY_DEAL_INFO 表 逆回购统计耗时 "+(time36- time35)/1000+" seconds")
    log.info("CTGRY_BOND_CTGRY_DEAL_INFO 表 逆回购统计耗时 "+(time36- time35)/1000+" seconds")

    /* 正回购
      机构在 评级上的分布 根机构机构 信用评级，每天，amount累加
    //  1        2                3              4              5            6                     9         10
 输入 (deal_dt,repo_pty_uniq,rvrse_repo_uniq, deal_repo_rate,trade_amount,bond_deal_info)
            //正回购机构，质押物类型，每天，amount
     */
    val repo_bond_credit_deal_rdd = repo_ev_joined_dtls.flatMap(record=>{
      val member_uniq = record._2 //正回购方
      val deal_date = record._1
      val trade_amount = record._5
      val bond_Infos = record._6
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],BigDecimal)]()
      for(info<- bond_Infos){
        val bond_id = info._1
        val ratio = info._3
        var bond_ctgry_nm = "其他"
        var rtng_desc = "AA及以下" //利率债没有评级，所以可以为None
        val ri_credit_rtng = broadRiCreditRtng.value
        val bond_ctgry_data = broadBondD.value
        if(ri_credit_rtng.contains(bond_id)){
          val bondRtngInfoList = ri_credit_rtng.get(bond_id).get
          var flag1 = true //没找到
          for(elem<-bondRtngInfoList if flag1){
            if(deal_date.compareTo(elem._1)>=0 && deal_date.compareTo(elem._2)<=0){
              rtng_desc = elem._3 //得到债券评级
              flag1 = false // 找到了
            }
          }
        }
        if(bond_ctgry_data.contains(bond_id)){
          val bondInfo = bond_ctgry_data.get(bond_id).get
          bond_ctgry_nm = bondInfo._1
        }
        arrays += Tuple2 (Tuple4(member_uniq,bond_ctgry_nm,rtng_desc,deal_date),
          ratio*trade_amount)
      }
      arrays.iterator  //机构ID，质押物类型，日期，amount
    }).filter(record=>{
      var flag = true
      if(record._1._2.equals("利率债")){ // 利率债没有评级
        flag = false
      }
      flag
    }).cache()

    // 根机构 评级统计 结果 写Hbase!!!!!
    repo_bond_credit_deal_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)

        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,Date],BigDecimal)]()
        arrays += Tuple2(Tuple5 (root_member_uniq,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple5 (root_member_uniq,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple5(root_member_uniq,"货币市场基金",record._1._2,record._1._3,record._1._4), record._2)
          }else{
            arrays += Tuple2(Tuple5 (root_member_uniq,"产品",record._1._2,record._1._3,record._1._4), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val deal_date = result._1._5
        (result._1._1,result._1._2,result._1._3,result._1._4,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_RTNG_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE","BOND_CTGRY_NM",
        "RTNG_DESC","DEAL_DT", "REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))
    val time37 = System.currentTimeMillis()
    println("MEMBER_BOND_RTNG_DEAL_INFO 表 正回购统计耗时 "+(time37- time36)/1000+" seconds")
    log.info("MEMBER_BOND_RTNG_DEAL_INFO 表 正回购统计耗时 "+(time37- time36)/1000+" seconds")

    // 机构类型 评级统计 结果 写Hbase!!!!!
    repo_bond_credit_deal_rdd
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
        }
        Tuple2(Tuple4(member_ins_show_nm,record._1._2,record._1._3,record._1._4),record._2)
      }).filter(x=>{!x._1._1.equals("None")})
      .reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val deal_date = result._1._4
        (result._1._1,result._1._2,result._1._3,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_BOND_RTNG_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM","RTNG_DESC", "DEAL_DT", "REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))
    val time38 = System.currentTimeMillis()
    println("CTGRY_BOND_RTNG_DEAL_INFO 表 正回购统计耗时 "+(time38- time37)/1000+" seconds")
    log.info("CTGRY_BOND_RTNG_DEAL_INFO 表 正回购统计耗时 "+(time38- time37)/1000+" seconds")

    /*
    逆回购 在质押物类型上的分布
 //  1        2                3              4              5            6                     9         10
输入 (deal_dt,repo_pty_uniq,rvrse_repo_uniq, deal_repo_rate,trade_amount,bond_deal_info)
     */
    val rvrse_repo_bond_credit_deal_rdd = repo_ev_joined_dtls.flatMap(record=>{
      val member_uniq = record._3 //逆回购方uniq
      val deal_date = record._1
      val trade_amount = record._5
      val bond_Infos = record._6
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],BigDecimal)]()
      for(info<- bond_Infos){
        val bond_id = info._1
        val ratio = info._3
        var bond_ctgry_nm = "其他"
        var rtng_desc = "AA及以下" //利率债没有评级，所以可以为None
        val ri_credit_rtng = broadRiCreditRtng.value
        val bond_ctgry_data = broadBondD.value
        if(ri_credit_rtng.contains(bond_id)){
          val bondRtngInfoList = ri_credit_rtng.get(bond_id).get
          var flag1 = true //没找到
          for(elem<-bondRtngInfoList if flag1){
            if(deal_date.compareTo(elem._1)>=0 && deal_date.compareTo(elem._2)<=0){
              rtng_desc = elem._3 //得到债券评级
              flag1 = false // 找到了
            }
          }
        }
        if(bond_ctgry_data.contains(bond_id)){
          val bondInfo = bond_ctgry_data.get(bond_id).get
          bond_ctgry_nm = bondInfo._1
        }

        arrays += Tuple2(Tuple4(member_uniq,bond_ctgry_nm,rtng_desc,deal_date), ratio*trade_amount)
      }
      arrays.iterator  //机构ID，质押物类型，日期，amount
    }).filter(record=>{
      var flag = true
      if(record._1._2.equals("利率债")){ // 利率债没有评级
        flag = false
      }
      flag
    }).cache()

    // 根机构 评级统计 结果 写Hbase!!!!!
    rvrse_repo_bond_credit_deal_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,Date],BigDecimal)]()
        arrays += Tuple2(Tuple5 (root_member_uniq,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple5 (root_member_uniq,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple5 (root_member_uniq,"货币市场基金",record._1._2,record._1._3,record._1._4), record._2)
          }else{
            arrays += Tuple2(Tuple5 (root_member_uniq,"产品",record._1._2,record._1._3,record._1._4), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val deal_date = result._1._5
        (result._1._1,result._1._2,result._1._3,result._1._4,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_RTNG_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE","BOND_CTGRY_NM",
        "RTNG_DESC","DEAL_DT", "RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    val time39 = System.currentTimeMillis()
    println("MEMBER_BOND_RTNG_DEAL_INFO 表 逆回购统计耗时 "+(time39 - time38)/1000+" seconds")
    log.info("MEMBER_BOND_RTNG_DEAL_INFO 表 逆回购统计耗时 "+(time39 - time38)/1000+" seconds")

    // 机构类型 评级统计 结果 写Hbase!!!!!
    rvrse_repo_bond_credit_deal_rdd
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
        }
        Tuple2(Tuple4(member_ins_show_nm,record._1._2,record._1._3,record._1._4),record._2)
      }).filter(x=>{!x._1._1.equals("None")})
      .reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val deal_date = result._1._4
        (result._1._1,result._1._2,result._1._3,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_BOND_RTNG_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM","RTNG_DESC","DEAL_DT",
        "RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    val time40 = System.currentTimeMillis()
    println("CTGRY_BOND_RTNG_DEAL_INFO 表 逆回购统计耗时 "+(time40- time39)/1000+" seconds")
    log.info("CTGRY_BOND_RTNG_DEAL_INFO 表 逆回购统计耗时 "+(time40- time39)/1000+" seconds")

    /*
     质押物折扣率走势图检索:债券类型检索
  //  1         2              3            4               5           6
 (deal_dt,repo_pty_uniq,rvrse_repo_uniq, deal_repo_rate,trade_amount,bond_deal_info)
 */
    val repo_bond_discount_rdd = repo_ev_joined_dtls.flatMap(record=>{
      val member_uniq = record._2 //正回购方
      val deal_date = record._1
      val trade_amount = record._5
      val bond_Infos = record._6
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],
        Tuple2[BigDecimal,BigDecimal])]()
      for(info<- bond_Infos){
        val bond_id = info._1
        val cnvrsn_prprtn  = info._2
        val ratio = info._3
        var bond_ctgry_nm = "其他"
        var rtng_desc = "AA及以下" //利率债没有评级，所以可以为None
        val ri_credit_rtng = broadRiCreditRtng.value
        val bond_ctgry_data = broadBondD.value
        if(ri_credit_rtng.contains(bond_id)){
          val bondRtngInfoList = ri_credit_rtng.get(bond_id).get
          var flag1 = true //没找到
          for(elem<-bondRtngInfoList if flag1){
            if(deal_date.compareTo(elem._1)>=0 && deal_date.compareTo(elem._2)<=0){
              rtng_desc = elem._3 //得到债券评级
              flag1 = false // 找到了
            }
          }
        }
        if(bond_ctgry_data.contains(bond_id)){
          val bondInfo = bond_ctgry_data.get(bond_id).get
          bond_ctgry_nm = bondInfo._1
        }
        val ratio_amount = ratio* trade_amount
        arrays += Tuple2 (Tuple4(member_uniq,bond_ctgry_nm,rtng_desc,deal_date),
          Tuple2(cnvrsn_prprtn * ratio_amount,ratio_amount))
      }
      arrays.iterator  //机构ID，质押物类型，日期，amount
    }).cache()
//      .filter(record=>{
//      var flag = true
//        if(record._1._2.equals("None")){ //分类信息找不到,由于利率债没有评级
//          flag =false
//      }
//      flag
//    }).cache()
  //  Tuple2 (   Tuple4(member_uniq,bond_ctgry_nm,rtng_desc,deal_date),
   //            Tuple2(cnvrsn_prprtn * ratio_amount,ratio_amount))

    ///按根机构 进行统计 结果写Hbase
    repo_bond_discount_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)

        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,Date],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple5 (root_member_uniq,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple5 (root_member_uniq,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple5 (root_member_uniq,"货币市场基金",record._1._2,record._1._3,record._1._4), record._2)
          }else{
            arrays += Tuple2(Tuple5 (root_member_uniq,"产品",record._1._2,record._1._3,record._1._4), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{
        val deal_date = result._1._5
        val RWCP = result._2._1/result._2._2
        (result._1._1,result._1._2,result._1._3,result._1._4,deal_date,RWCP.bigDecimal)})
      .saveToPhoenix("MEMBER_BOND_TYPE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE", "BOND_CTGRY_NM",
        "RTNG_DESC","DEAL_DT", "REPO_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))

    val time41 = System.currentTimeMillis()
    println("MEMBER_BOND_TYPE_DEAL_INFO 表 正回购统计耗时 "+(time41- time40)/1000+" seconds")
    log.info("MEMBER_BOND_TYPE_DEAL_INFO 表 正回购统计耗时 "+(time41- time40)/1000+" seconds")

    //按机构类型 进行统计 结果写Hbase
    repo_bond_discount_rdd.map(record=>{
      val member_uniq = record._1._1
      var member_ins_show_nm = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
      }
      Tuple2(Tuple4(member_ins_show_nm,record._1._2,record._1._3,record._1._4),record._2)
    }).filter(x=>{!x._1._1.equals("None")})
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{
        val deal_date = result._1._4
        val RWCP = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,result._1._3,deal_date,RWCP)
      })//无需写amount
      .saveToPhoenix("CTGRY_BOND_TYPE_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM",
      "RTNG_DESC","DEAL_DT", "REPO_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))

    val time42 = System.currentTimeMillis()
    println("CTGRY_BOND_TYPE_DEAL_INFO 表 正回购统计耗时 "+(time42- time41)/1000+" seconds")
    log.info("CTGRY_BOND_TYPE_DEAL_INFO 表 正回购统计耗时 "+(time42- time41)/1000+" seconds")

    /*
    对逆回购进行分析
 //  1         2              3            4               5           6
 (deal_dt,repo_pty_uniq,rvrse_repo_uniq, deal_repo_rate,trade_amount,bond_deal_info)
*/
    val rvrse_repo_bond_discount_rdd = repo_ev_joined_dtls.flatMap(record=>{
      val member_uniq = record._3 //逆回购方
      val deal_date = record._1
      val trade_amount = record._5
      val bond_Infos = record._6
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],
        Tuple2[BigDecimal,BigDecimal])]()
      for(info<- bond_Infos){
        val bond_id = info._1
        val cnvrsn_prprtn  = info._2
        val ratio = info._3
        var bond_ctgry_nm = "其他"
        var rtng_desc = "AA及以下" //利率债没有评级，所以可以为None
        val ri_credit_rtng = broadRiCreditRtng.value
        val bond_ctgry_data = broadBondD.value
        if(ri_credit_rtng.contains(bond_id)){
          val bondRtngInfoList = ri_credit_rtng.get(bond_id).get
          var flag1 = true //没找到
          for(elem<-bondRtngInfoList if flag1){
            if(deal_date.compareTo(elem._1)>=0 && deal_date.compareTo(elem._2)<=0){
              rtng_desc = elem._3 //得到债券评级
              flag1 = false // 找到了
            }
          }
        }
        if(bond_ctgry_data.contains(bond_id)){
          val bondInfo = bond_ctgry_data.get(bond_id).get
          bond_ctgry_nm = bondInfo._1
        }
        val ratio_amount = ratio* trade_amount
        arrays += Tuple2(Tuple4(member_uniq,bond_ctgry_nm,rtng_desc,deal_date),
          Tuple2(cnvrsn_prprtn * ratio_amount,ratio_amount))
      }
      arrays.iterator  //机构ID，质押物类型，日期，amount
    }).cache()

    ///按根机构 进行统计 结果写Hbase
    rvrse_repo_bond_discount_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
        val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,Date],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple5 (root_member_uniq,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple5 (root_member_uniq,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          var member_full_name = "None"
          val memberUniq2Info = broadMemerUniq2Info.value
          if(!memberUniq2Info.contains(member_uniq)){
            log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
          }else{
            member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple5 (root_member_uniq,"货币市场基金",record._1._2,record._1._3,record._1._4), record._2)
          }else{
            arrays += Tuple2(Tuple5 (root_member_uniq,"产品",record._1._2,record._1._3,record._1._4), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{
        val deal_date = result._1._5
        val RWCP = result._2._1/result._2._2
        (result._1._1,result._1._2,result._1._3,result._1._4,deal_date, RWCP.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_TYPE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","RTNG_DESC","DEAL_DT", "RVRSE_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))

    val time43 = System.currentTimeMillis()
    println("MEMBER_BOND_TYPE_DEAL_INFO 表 逆回购统计耗时 "+(time43- time42)/1000+" seconds")
    log.info("MEMBER_BOND_TYPE_DEAL_INFO 表 逆回购统计耗时 "+(time43- time42)/1000+" seconds")


    //按机构类型 进行统计 结果写Hbase
    rvrse_repo_bond_discount_rdd.map(record=>{
      val member_uniq = record._1._1
      var member_ins_show_nm = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
      }
      Tuple2(Tuple4(member_ins_show_nm,record._1._2,record._1._3,record._1._4),record._2)
    }).filter(x=>{!x._1._1.equals("None")})
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{
        val deal_date = result._1._4
        val RWCP = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,result._1._3,deal_date,RWCP)
      })//无需写amount
      .saveToPhoenix("CTGRY_BOND_TYPE_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM",
      "RTNG_DESC","DEAL_DT", "RVRSE_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))
    val time44 = System.currentTimeMillis()
    println("CTGRY_BOND_TYPE_DEAL_INFO 表 逆回购统计耗时 "+(time44- time43)/1000+" seconds")
    log.info("CTGRY_BOND_TYPE_DEAL_INFO 表 逆回购统计耗时 "+(time44- time43)/1000+" seconds")
    /*
     质押物折扣率走势图检索:发行机构检索     正回购
   //  1      2      3               4              5           6          7            8            9         10
// (pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,deal_repo_rate,trade_amount,bond_id,cnvrsn_prprtn)

 */
    val repo_bond_discount_issr_rdd = repo_ev_joined_dtls.flatMap(record=>{
      val member_uniq = record._2 //正回购方
      val deal_date = record._1
      val trade_amount = record._5
      val bond_Infos = record._6
      val arrays = new ArrayBuffer[(Tuple3[String,String,Date],
        Tuple2[BigDecimal,BigDecimal])]()
      for(info<- bond_Infos){
        val bond_id = info._1
        val cnvrsn_prprtn  = info._2
        val ratio = info._3
        var issr_id = "None"
        val bond_ctgry_data = broadBondD.value
        if(!bond_ctgry_data.contains(bond_id)){
          log.warn("broadBondD does not contain bond_id:"+bond_id)
        } else {
          val bond_info = bond_ctgry_data.get(bond_id).get
          issr_id = bond_info._2
        }
        //key:          机构ID，  发行人名称，日期；             value：加权折扣率，amount
        val ratio_amount = ratio* trade_amount
        arrays += Tuple2 (Tuple3(member_uniq,issr_id,deal_date),
          Tuple2(cnvrsn_prprtn * ratio_amount,ratio_amount))
      }
      arrays.iterator  //机构ID，质押物类型，日期，amount
    }).filter(x=>{!x._1._2.equals("None")})
      .cache()

    ///按根机构 进行统计 结果写Hbase
    repo_bond_discount_issr_rdd.flatMap(record=>{
      val member_uniq = record._1._1
      val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
      val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)

      val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],Tuple2[BigDecimal,BigDecimal])]()
      arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
      if(member_uniq.equals(root_member_uniq)){
        arrays += Tuple2(Tuple4 (root_member_uniq,"机构",record._1._2,record._1._3), record._2)
      }else{
        var member_full_name = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
        }else{
          member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
        }
        if(member_full_name.contains("货币市场基金")){
          arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
        }
      }
      arrays.iterator
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(record=>{
        val deal_date = record._1._4
        val RWCP = (record._2._1/record._2._2).bigDecimal
   //     (record._1._1,record._1._2,record._1._3,deal_date,RWCP,record._2._2.bigDecimal)
        (record._1._1,record._1._2,record._1._3,deal_date,RWCP)
      })
      .saveToPhoenix("MEMBER_BOND_ISSR_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "ISSR_ID","DEAL_DT","REPO_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))

    val time45 = System.currentTimeMillis()
    println("MEMBER_BOND_ISSR_DEAL_INFO 表 正回购统计耗时 "+(time45- time44)/1000+" seconds")
    log.info("MEMBER_BOND_ISSR_DEAL_INFO 表 正回购统计耗时 "+(time45- time44)/1000+" seconds")

    //按机构类型 进行统计 结果写Hbase
    repo_bond_discount_issr_rdd.map(record=>{
      val member_uniq = record._1._1
      var member_ins_show_nm = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
      }
      Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .filter(x=>{!x._1._1.equals("None")})
      .map(result=>{
        val deal_date = result._1._3
        val RWCP = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,deal_date,RWCP)
      })//无需写amount
      .saveToPhoenix("CTGRY_BOND_ISSR_DEAL_INFO",Seq("INS_SHOW_NAME", "ISSR_ID","DEAL_DT",
      "REPO_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))
    val time46 = System.currentTimeMillis()
    println("CTGRY_BOND_ISSR_DEAL_INFO 表 正回购统计耗时 "+(time46- time45)/1000+" seconds")
    log.info("CTGRY_BOND_ISSR_DEAL_INFO 表 正回购统计耗时 "+(time46- time45)/1000+" seconds")

    val rvrse_repo_bond_discount_issr_rdd = repo_ev_joined_dtls.flatMap(record=>{
      val member_uniq = record._3 //正回购方
      val deal_date = record._1
      val trade_amount = record._5
      val bond_Infos = record._6
      val arrays = new ArrayBuffer[(Tuple3[String,String,Date],
        Tuple2[BigDecimal,BigDecimal])]()
      for(info<- bond_Infos){
        val bond_id = info._1
        val cnvrsn_prprtn  = info._2
        val ratio = info._3
        var issr_id = "None"
        val bond_ctgry_data = broadBondD.value
        if(!bond_ctgry_data.contains(bond_id)){
          log.warn("broadBondD does not contain bond_id:"+bond_id)
        } else {
          val bond_info = bond_ctgry_data.get(bond_id).get
          issr_id = bond_info._2
        }
        //key:          机构ID，  发行人名称，日期；             value：加权折扣率，amount
        val ratio_amount = ratio* trade_amount
        arrays += Tuple2 (Tuple3(member_uniq,issr_id,deal_date),
          Tuple2(cnvrsn_prprtn * ratio_amount,ratio_amount))
      }
      arrays.iterator  //机构ID，质押物类型，日期，amount
    }).filter(x=>{!x._1._2.equals("None")})
      .cache()


    ///按根机构 进行统计 结果写Hbase
    rvrse_repo_bond_discount_issr_rdd.flatMap(record=>{
      val member_uniq = record._1._1
      val memberUniq2RootUniq = broadMemberUniq2RootUniq.value
      val root_member_uniq = memberUniq2RootUniq.getOrElse(member_uniq,member_uniq)

      val arrays = new ArrayBuffer[(Tuple4[String,String,String,Date],Tuple2[BigDecimal,BigDecimal])]()
      arrays += Tuple2(Tuple4 (root_member_uniq,"全部",record._1._2,record._1._3), record._2)
      if(member_uniq.equals(root_member_uniq)){
        arrays += Tuple2(Tuple4 (root_member_uniq,"机构",record._1._2,record._1._3), record._2)
      }else{
        var member_full_name = "None"
        val memberUniq2Info = broadMemerUniq2Info.value
        if(!memberUniq2Info.contains(member_uniq)){
          log.warn("broadMemerUniq2Info doesnot contain:"+member_uniq)
        }else{
          member_full_name= memberUniq2Info.get(member_uniq).get._1//得到机构全称
        }
        if(member_full_name.contains("货币市场基金")){
          arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
        }
      }
      arrays.iterator
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(record=>{
        val deal_date = record._1._4
        val RWCP = (record._2._1/record._2._2).bigDecimal
        (record._1._1,record._1._2,record._1._3,deal_date,RWCP)
      })
      .saveToPhoenix("MEMBER_BOND_ISSR_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE", "ISSR_ID",
        "DEAL_DT","RVRSE_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))

    val time47 = System.currentTimeMillis()
    println("MEMBER_BOND_ISSR_DEAL_INFO 表 逆回购统计耗时 "+(time47- time46)/1000+" seconds")
    log.info("MEMBER_BOND_ISSR_DEAL_INFO 表 逆回购统计耗时 "+(time47- time46)/1000+" seconds")

    //按机构类型 进行统计 结果写Hbase
    rvrse_repo_bond_discount_issr_rdd.map(record=>{
      val member_uniq = record._1._1
      var member_ins_show_nm = "None"
      val memberUniq2Info = broadMemerUniq2Info.value
      if(!memberUniq2Info.contains(member_uniq)){
        log.warn("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = memberUniq2Info.get(member_uniq).get._2
      }
      Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>{(x._1+y._1,x._2+y._2)})
      .filter(x=>{!x._1._1.equals("None")})
      .map(result=>{
        val deal_date = result._1._3
        val value = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,deal_date,value)
      })//无需写amount
      .saveToPhoenix("CTGRY_BOND_ISSR_DEAL_INFO",Seq("INS_SHOW_NAME", "ISSR_ID",
      "DEAL_DT", "RVRSE_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))
    val time48 = System.currentTimeMillis()
    println("CTGRY_BOND_ISSR_DEAL_INFO 表 逆回购统计耗时 "+(time48- time47)/1000+" seconds")
    log.info("CTGRY_BOND_ISSR_DEAL_INFO 表 逆回购统计耗时 "+(time48- time47)/1000+" seconds")

    println("total running time "+(time48- beginTime)/1000+" seconds")
    log.info("total running time "+(time48- beginTime)/1000+" seconds")
    sc.stop()
  }


}

