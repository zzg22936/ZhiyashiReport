package com.cfetsit.bigdata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.log4j.Logger
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object BondReport {

  @transient lazy val  log = Logger.getLogger(this.getClass)

  def Bond_Maturity_Transformation(Bond_Maturity:BigDecimal): String ={
    var Bond_Maturity_Type = ""
    if(Bond_Maturity>math.BigDecimal.int2bigDecimal(10)){
      Bond_Maturity_Type = "十年以上"
    }
    else if(Bond_Maturity>math.BigDecimal.int2bigDecimal(7) && Bond_Maturity<=math.BigDecimal.int2bigDecimal(10)){
      Bond_Maturity_Type ="七年至十年"
    }
    else if(Bond_Maturity>math.BigDecimal.int2bigDecimal(5) && Bond_Maturity<=math.BigDecimal.int2bigDecimal(7)){
      Bond_Maturity_Type ="五年至七年"
    }else if(Bond_Maturity>math.BigDecimal.int2bigDecimal(3) && Bond_Maturity<=math.BigDecimal.int2bigDecimal(5)){
      Bond_Maturity_Type ="三年至五年"
    }
    else if(Bond_Maturity>math.BigDecimal.int2bigDecimal(1) && Bond_Maturity<=math.BigDecimal.int2bigDecimal(3)){
      Bond_Maturity_Type ="一年至三年"
    }
    else
    {
      Bond_Maturity_Type ="一年及以下"
    }
    Bond_Maturity_Type
  }

  def transfer(sc:SparkContext,path:String):RDD[String]={
    sc.hadoopFile(path,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }


  def Ins_Show_Name_Transformation(Ins_Show_Name:String): String ={
    var ins_show_name = Ins_Show_Name
    val a = ins_show_name.indexOf("(")
    val b = ins_show_name.indexOf("（")
    val c = math.max(a,b)
    if(c>0){
      ins_show_name = ins_show_name.substring(0,c).trim
    }
    ins_show_name
  }

  def main(args: Array[String]): Unit = {

    //val sparkConf = new SparkConf().setAppName("BondReport").setMaster("local[2]")
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    // val ZOOKEEPER_URL="127.0.0.1:2181"
    val ZOOKEEPER_URL=args(0)
    val conf = new Configuration()
    log.info("zookeeper url:"+args(0))
    println("zookeeper url:"+args(0))


    //CIM mstr_slv_rl_tp_rl 表
    val mstr_slv_rl_tp_rl_data = sc.textFile(args(7))
      .filter(_.length!=0).filter(!_.contains("MSTR"))
      .filter(line=>{
        val lineArray = Utils.split(line)
        var flag = true
        if(lineArray.size()!=4){ flag = false }
        else{
          for(i<-0 until lineArray.size() if flag){
            if(lineArray.get(i)==null || lineArray.get(i).isEmpty) {flag =false}
          }
          if(!lineArray.get(0).equals("1")){
            flag =false
          }
          if(!lineArray.get(2).startsWith("9999-12-31")){
            flag = false
          }
        }
        flag
      }).map(line=> {
      val tmp = Utils.split(line)
      tmp.get(1)
    }).collect().toSet

    val broadMSTR_SLV_RL_TP_RL = sc.broadcast(mstr_slv_rl_tp_rl_data)
    log.info("file 7 mstr_slv_rl_tp_rl_data read succeed! read "+mstr_slv_rl_tp_rl_data.size+" records")
    println("file 7 mstr_slv_rl_tp_rl_data read succeed! read "+mstr_slv_rl_tp_rl_data.size+" records")

    //主从机构表
    val cim_org_rl_data = sc.textFile(args(8))
      .filter(_.length!=0).filter(!_.contains("ORG"))
      .filter(line=> {
        val lineList = Utils.split(line)
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
          val cim_mstr_slv_data = broadMSTR_SLV_RL_TP_RL.value
          val org_rl_tp_id = lineList.get(0)
          if(flag && !cim_mstr_slv_data.contains(org_rl_tp_id)){
            flag = false
          }
        }
        flag
      })
      .map(line=>{
        val lineList = Utils.split(line)
        val SLV_ORG_ID = lineList.get(1)
        val MSTR_ORG_ID = lineList.get(2)
        (SLV_ORG_ID,MSTR_ORG_ID)
      }).collectAsMap()

    val broadCIM_ORG_RL = sc.broadcast(cim_org_rl_data)
    log.info("file 8 cim_org_rl_data read succeed! read "+cim_org_rl_data.size+" records")
    println("file 8 cim_org_rl_data read succeed! read "+cim_org_rl_data.size+" records")

    //过滤交易维度表，和现券交易信息表进行关联
    val trdx_deal_infrmn_rmv_d_data = sc.textFile(args(6))
      .filter(_.length!=0).filter(!_.contains("DEAL")).filter(line=> {
      val lineList = Utils.split(line)

      var flag = true
      if(lineList.size() != 5){
        flag =  false
      }else{
        for(i<-0 until lineList.size() if flag){
          if(lineList.get(i)==null || lineList.get(i).isEmpty){
            flag =false
          }
        }
        if(flag){
          if(!lineList.get(0).equals("2") || !lineList.get(1).equals("1") ){
            flag = false
          }
        }
      }
      flag
    })
      .map(line=> {
        val tmp = Utils.split(line)
        tmp.get(3)
      }).collect().toSet

    //print("---------trdx_deal_infrmn_rmv_d_data----------")
    //trdx_deal_infrmn_rmv_d_data.take(10).foreach { println }
    log.info("file 6 trdx_deal_infrmn_rmv_d_data read succeed! read "+trdx_deal_infrmn_rmv_d_data.size+" records")
    println("file 6 trdx_deal_infrmn_rmv_d_data read succeed! read "+trdx_deal_infrmn_rmv_d_data.size+" records")

    val broadTRDX_DEAL_FAIL_ID = sc.broadcast(trdx_deal_infrmn_rmv_d_data)

    /*
   盘后债券类型
     */
    val after_hours_bond_type_data = sc.textFile(args(3))
      .filter(_.length!=0).filter(!_.contains("MEMBER"))
      .filter(line=>{
        val lineArray = Utils.split(line)
        var flag = true
        if(lineArray.size() != 2){
          flag = false
        }else{
          for(i<-0 until lineArray.size() if flag) {
            if (lineArray.get(i) == null || lineArray.get(i).isEmpty) {
              flag = false
            }
          }
        }
        flag
      }).map(line=>{
      val tmp = Utils.split(line)
      val ins_show_name = Ins_Show_Name_Transformation(tmp.get(0))
      //(ins_show_name, member_ctgry_id)
      (tmp.get(1),ins_show_name)
    }).collectAsMap()

    //print("---------after_hours_bond_type_data----------")
    //after_hours_bond_type_data.take(10).foreach { println }
    log.info("file 3 after_hours_bond_type_data read succeed! read "+after_hours_bond_type_data.size+" records")
    println("file 3 after_hours_bond_type_data read succeed! read "+after_hours_bond_type_data.size+" records")

    val broadAFTER_HOURS_BOND_TYPE = sc.broadcast(after_hours_bond_type_data)

    /*
   构建机构的member_d数据，增加机构的盘后类型名(可展示类型。)
    */
    val member_d_rdd = sc.textFile(args(2))
      .filter(_.length!=0).filter(!_.contains("MEMBER"))
      .filter(line=>{
        val lineArray = Utils.split(line)
        var flag = true
        if(lineArray.size() != 10){
          flag = false
        }else{
          for(i<-0 until lineArray.size() if flag){
            //根机构代码和根机构全称可能为空
            if(lineArray.get(i)==null || lineArray.get(i).isEmpty){
              if(i!=4 && i!=5) {flag = false}
            }
          }
          if(flag && !lineArray.get(7).equals("9999-12-31")) {flag=false}
        }
        flag
      })
      .map(line=>{
        val tmp =  Utils.split(line)
        var ins_show_name = "None"
        val member_ctgry_id = tmp.get(1)

        if(!broadAFTER_HOURS_BOND_TYPE.value.contains(member_ctgry_id)) {
          log.warn("broadAfterHoursBondType does not contain member_ctgry_id:"+member_ctgry_id)
          println("broadAfterHoursBondType does not contain member_ctgry_id:"+member_ctgry_id)
        }
        else {
          ins_show_name = broadAFTER_HOURS_BOND_TYPE.value.get(member_ctgry_id).get
          if(ins_show_name==null){
            ins_show_name="NULL"
            log.warn("broadAfterHoursBondType get member_ctgry_id:"+member_ctgry_id+"is null! Add For Uat Data")
            println("broadAfterHoursBondType get member_ctgry_id:"+member_ctgry_id+"is null! Add For Uat Data")
          }
        }
        (tmp.get(0), (tmp.get(1),tmp.get(2),tmp.get(3),tmp.get(4),tmp.get(5),tmp.get(6),tmp.get(7),ins_show_name,tmp.get(8)))
      }).cache()

    val member_d_data = member_d_rdd.collectAsMap()


    //print("---------member_d_data----------")
    //member_d_data.take(10).foreach { println }
    log.info("file 2 member_d_data read succeed! read "+member_d_data.size+" records")
    println("file 2 member_d_data read succeed! read "+member_d_data.size+" records")

    val broadMEMBER_D = sc.broadcast(member_d_data)

    //    val member_effective_fromToDate = member_d_rdd.map(record=>{
    //      val ip_id = record._1
    //      val oracleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //      val fromDate = oracleDateFormat.parse(record._2._6)
    //      val toDate = oracleDateFormat.parse(record._2._7)
    //      (ip_id,Set(Tuple2(fromDate,toDate)))
    //    }).reduceByKey((x,y)=>x++y).collectAsMap()
    //    log.info("member_effective_fromToDate succeed! read "+member_effective_fromToDate.size+" records")
    //    println("member_effective_fromToDate succeed! read "+member_effective_fromToDate.size+" records")
    //
    //    val broadMEMBER_EFFECTIVE_FROMTODATE = sc.broadcast(member_effective_fromToDate)

    val member_effective = member_d_rdd.map(record=>{
      val ip_id = record._1
    }).collect().toSet

    val broadMemberEffective = sc.broadcast(member_effective)

    val memberUniqToId = member_d_rdd.map(record=>{
      val member_id = record._1
      val member_uniq = record._2._9
      (member_uniq,member_id)
    }).collectAsMap()

    log.info("memberUniqToId process succeed! read "+memberUniqToId.size+" records")
    println("memberUniqToId process succeed! read "+memberUniqToId.size+" records")

    val broadMEMBER_UNIQ_TO_ID = sc.broadcast(memberUniqToId)

    val memberID2Name = member_d_rdd.map(record=>{
      val memmber_id = record._1
      //  val rt_member_cd = record._2._4
      val ins_show_name = record._2._8
      (memmber_id,ins_show_name)
    })

    memberID2Name.saveToPhoenix("MEMBERID2NAME",Seq("MEMBER_ID","CTGRY_TYPE"),conf,Some(ZOOKEEPER_URL))
    val memberID2NameMap = memberID2Name.collectAsMap()
    //memberID2NameMap.take(10).foreach { println }

    log.info("memberID2NameMap map succeed! read "+memberID2NameMap.size+" records")
    println("memberID2NameMap map succeed! read "+memberID2NameMap.size+" records")
    val broadMemberID2Name = sc.broadcast(memberID2NameMap)

    /*
    Bond_d 中 bond_ctgry_nm进行变换。【3种分类】
    */
    val bond_d_data_1 = sc.textFile(args(4))
      .filter(_.length!=0).filter(!_.contains("BOND"))
      .filter(line=>{
        val lineArray = Utils.split(line)
        var flag = true

        if(lineArray.size() != 14){
          flag = false
        }
        else {
          for (i <- 0 until lineArray.size() if flag) {
            if (lineArray.get(i)==null || lineArray.get(i).isEmpty) {
              flag = false
            }
          }
        }
        flag
      }).map(line=>{
      val tmp = Utils.split(line)
      if(tmp.get(13).equals("利率债")){
        (tmp.get(0),(tmp.get(4),"利率债",tmp.get(5),tmp.get(8),tmp.get(12)))
      }else if(tmp.get(13).equals("同业存单")){
        (tmp.get(0),(tmp.get(4),"同业存单",tmp.get(5),tmp.get(8),tmp.get(12)))
      }else if(tmp.get(13).equals("信用债")){
        (tmp.get(0),(tmp.get(4),"信用债",tmp.get(5),tmp.get(8),tmp.get(12)))
      }else{
        (tmp.get(0),(tmp.get(4),"其他",tmp.get(5),tmp.get(8),tmp.get(12)))
      }
    })

    val bondInfo = bond_d_data_1
      .map(x=>{
        (x._1,x._2._1,x._2._3,x._2._4,x._2._5)
      }).map(record=>{
      //      val oracleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      //      val lstng_date = oracleDateFormat.parse(record._5)
      //      val mrty_date = oracleDateFormat.parse(record._4)

      //      (record._1,record._2,record._3,lstng_date,mrty_date)
      (record._1,record._2,record._3,record._5,record._4)
    })
      .saveToPhoenix("BONDINFO",Seq("BOND_ID","BOND_NM","BOND_CD","LSTNG_DT","MRTY_DT"),conf,Some(ZOOKEEPER_URL))

    val bond_d_data_1_Map = bond_d_data_1.collectAsMap()

    log.info("file 4 bond_d_data_1 read succeed! read "+bond_d_data_1_Map.size+" records")
    println("file 4 bond_d_data_1 read succeed! read "+bond_d_data_1_Map.size+" records")

    val broadBondD_1 = sc.broadcast(bond_d_data_1_Map)

    /*
    Bond_d 中 bond_ctgry_nm进行变换。【8种分类】
     */
    val bond_d_data_2 = sc.textFile(args(4))
      .filter(_.length!=0).filter(!_.contains("BOND"))
      .filter(line=> {
        val lineArray = Utils.split(line)
        var flag = true

        if (lineArray.size() != 14) {
          flag = false
        }
        else {
          for (i <- 0 until 5 if flag) {
            if (lineArray.get(i)==null || lineArray.get(i).isEmpty) {
              flag = false
            }
          }
        }
        flag
      }).map(line=>{
      val tmp = Utils.split(line)
      var bondType = "其他"

      if(tmp.get(1).equals("国债")){
        bondType = "国债"
      }else if(tmp.get(1).equals("同业存单")) {
        bondType = "同业存单"
      }else if(tmp.get(1).equals("政策性金融债")){
        bondType ="政策性金融债"
      }else if(tmp.get(1).equals("地方政府债")){
        bondType ="地方政府债"
      } else if(tmp.get(1).equals("超短期融资券") || tmp.get(1).equals("短期融资券")){
        bondType ="短期融资券（包含超短融）"
      }else if(tmp.get(1).equals("中期票据")){
        bondType ="中期票据"
      }else if(tmp.get(1).equals("企业债")){
        bondType ="企业债"
      }else{
        bondType = "其他"
      }
      (tmp.get(0),(tmp.get(4),bondType))
    }).collectAsMap()

    log.info("file 4 bond_d_data_2 read succeed! read "+bond_d_data_2.size+" records")
    println("file 4 bond_d_data_2 read succeed! read "+bond_d_data_2.size+" records")

    val broadBondD_2 = sc.broadcast(bond_d_data_2)

    /*
    交易策略
     */
    val dps_v_cbt_all_txn_dtl_data = sc.textFile(args(5))
      .filter(_.length!=0).filter(!_.contains("DBD"))
      .filter(line=>{
        val lineArray = Utils.split(line)
        var flag = true
        if(lineArray.size() != 2){
          flag = false
        }
        else{
          for(i<-0 until lineArray.size() if flag){
            if(lineArray.get(i)==null || lineArray.get(i).isEmpty()){
              flag = false
            }
          }
        }
        flag
      }).map(line=>{
      val tmp = Utils.split(line)
      var deal_type = "策略性交易"
      //【0- 待匹配，4-正常，null】
      if(tmp.get(1).isEmpty || tmp.get(1).equals("0") || tmp.get(1).equals("4"))
      {
        deal_type = "正常"
      }
      else deal_type = "策略性交易"

      (tmp.get(0),deal_type)
    }).collectAsMap()

    log.info("file 5 dps_v_cbt_all_txn_dtl_data read succeed! read "+dps_v_cbt_all_txn_dtl_data.size+" records")
    println("file 5 dps_v_cbt_all_txn_dtl_data read succeed! read "+dps_v_cbt_all_txn_dtl_data.size+" records")

    val broadDpsCbtAllTxnDtlData = sc.broadcast(dps_v_cbt_all_txn_dtl_data)


    /*
    现券明细数据过滤
     */
    val bond_deal_rdd = sc.textFile(args(1))
      .filter(_.length != 0).filter(!_.contains("BOND"))
      .filter(line=>{
        val lineArray = Utils.split(line)

        if(lineArray.size() != 15){
          false
        }
        else{
          var flag = true
          for(i<-0 until 12 if flag){
            if(lineArray.get(i)==null || lineArray.get(i).isEmpty){
              flag = false
            }
          }
          if(flag){
            if(lineArray.get(5).equals("2")){
              flag = false
            }
            if(flag && !lineArray.get(7).trim().equals("CNY") && !lineArray.get(7).trim().equals("-")){
              flag = false
            }
            if(flag){
              val buy_id = lineArray.get(1)
              val sell_id = lineArray.get(2)
              val effective_member = broadMemberEffective.value
              //if(broadMEMBER_EFFECTIVE_FROMTODATE.value.get(buy_id) == None || broadMEMBER_EFFECTIVE_FROMTODATE.value.get(sell_id) == None){
              if(!effective_member.contains(buy_id)  || !effective_member.contains(sell_id)){
                log.warn("member_d table does not contain buy_id:"+buy_id+" or sell_id:"+sell_id+"!")
                println("member_d table does not contain buy_id:"+buy_id+" or sell_id:"+sell_id+"!")
                flag = false
              }
            }
          }
          //          if(flag){
          //            val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
          //            val deal_date = oracleDateFormat.parse(lineArray.get(4))
          //
          //            var buy_flag = false
          //            var buy_id = lineArray.get(1)
          //            var buy_infor_set= broadMEMBER_EFFECTIVE_FROMTODATE.value.get(buy_id).get
          //
          //            for(curRecord<- buy_infor_set if !buy_flag){
          //              if(deal_date.after(curRecord._1) && deal_date.before(curRecord._2)){
          //                buy_flag = true
          //              }
          //            }
          //
          //            var sell_flag = false
          //            var sell_id = lineArray.get(2)
          //            val sell_infor_set = broadMEMBER_EFFECTIVE_FROMTODATE.value.get(sell_id).get
          //
          //            for(curRecord<- sell_infor_set if (!sell_flag)){
          //              if(deal_date.after(curRecord._1) && deal_date.before(curRecord._2)){
          //                sell_flag = true
          //              }
          //            }
          //            flag = (buy_flag && sell_flag)
          //          }
          flag
        }
      })
      .map(line=>{
        val tmp = Utils.split(line)
        (tmp.get(0), (tmp.get(1),tmp.get(2),tmp.get(3),tmp.get(4),tmp.get(5),tmp.get(6),tmp.get(7).trim(),tmp.get(8),tmp.get(9),tmp.get(10),tmp.get(11)))
      })

    log.info("bond_deal_rdd read succeed!",bond_deal_rdd.count()," records")
    println("bond_deal_rdd read succeed!",bond_deal_rdd.count()," records")


    //与broadTRDx相交，过滤后得到成功记录
    val bond_repo_deal_succed_records_rdd = bond_deal_rdd.filter(x=>{
      val fliter_trade_values = broadTRDX_DEAL_FAIL_ID.value
      var flag = true
      if(fliter_trade_values.contains(x._1)){
        flag = false
      }
      flag
    })

    log.info("bond_repo_deal_succed_records_rdd succeed!"+bond_repo_deal_succed_records_rdd.count()+" records")
    println("bond_repo_deal_succed_records_rdd succeed!"+bond_repo_deal_succed_records_rdd.count()+" records")


    /*
    对bond_deal清洗后的数据进行映射得到 结果相关属性
     */
    val bond_deal_table =  bond_repo_deal_succed_records_rdd.map(
      x=>{
        val buy_id = x._2._1
        val sell_id = x._2._2
        val deal_numbr = x._2._3

        var deal_strategy = "正常"
        if(broadDpsCbtAllTxnDtlData.value.contains(deal_numbr)){
          deal_strategy = broadDpsCbtAllTxnDtlData.value.get(deal_numbr).get
        }

        val deal_dt = x._2._4
        val bond_id = x._2._6

        var bond_type = "empty"
        if(!broadBondD_1.value.contains(bond_id)){
          log.warn("broadBondD_1 does not contain bond_id:"+bond_id)
          println("broadBondD_1 does not contain bond_id:"+bond_id)
        }else{
          bond_type = broadBondD_1.value.get(bond_id).get._2
        }

        var bond_type2 = "empty"
        if(!broadBondD_2.value.contains(bond_id)){
          log.warn("broadBondD_2 does not contain bond_id:"+bond_id)
          println("broadBondD_2 does not contain bond_id:"+bond_id)
        }else{
          bond_type2 = broadBondD_2.value.get(bond_id).get._2
        }

        val instmt_crncy = x._2._7
        val trade_amnt = x._2._8
        val dirty_amnt = x._2._9
        val ttl_face_value = x._2._10
        val yr_of_time_to_mrty = x._2._11
        val yr_of_time_to_mrty_type = Bond_Maturity_Transformation(BigDecimal(x._2._11))

        (x._1,buy_id,sell_id,deal_strategy,deal_numbr,deal_dt,bond_type,instmt_crncy,BigDecimal.apply(trade_amnt)
          ,BigDecimal.apply(dirty_amnt),BigDecimal.apply(ttl_face_value),BigDecimal(yr_of_time_to_mrty),bond_id,bond_type2,yr_of_time_to_mrty_type)
      }
    ).cache()

    /*
    <买方机构ID,债券类型【4分类】，交易日期 >  <成交金额，成交笔数>
     */
    val buyer_day = bond_deal_table
      .map(x=>{Tuple2(Tuple3(x._2,x._7,x._6),Tuple2(x._10,Set(x._5)))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2++y._2))

    /*
   <卖方机构ID,债券类型【4分类】，交易日期 >  <成交金额，成交笔数 成交编号集合>
    */
    val seller_day = bond_deal_table
      .map(x=>{Tuple2(Tuple3(x._3,x._7,x._6),Tuple2(x._10,Set(x._5)))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2++y._2))

    /*
    <买方机构ID,参与者类型，债券类型【4分类】，交易日期> <成交金额，成交笔数>
     */
    val root_buyer_day = buyer_day
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,Set[String]])]()
        arrays += Tuple2(Tuple4 (root_member_id,"全部",record._1._2,record._1._3), record._2)
        if(isRT){
          arrays += Tuple2(Tuple4 (root_member_id,"机构",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_id,"产品",record._1._2,record._1._3), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2++y._2))

    /*
   <卖方机构ID,参与者类型，债券类型【4分类】，交易日期> <成交金额，成交笔数>
    */
    val root_seller_day = seller_day
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,Set[String]])]()
        arrays += Tuple2(Tuple4 (root_member_id,"全部",record._1._2,record._1._3), record._2)
        if(isRT){
          arrays += Tuple2(Tuple4 (root_member_id,"机构",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_id,"产品",record._1._2,record._1._3), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2++y._2))


    /*
    <机构ID,参与者类型，债券类型【4分类】，交易日期> (<成交金额，成交笔数>)
     */
    val root_deal_amount_trans_info = (root_buyer_day union root_seller_day)
      .reduceByKey((x,y)=>(x._1+y._1, x._2++y._2))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._4)
        val deal_date = record._1._4
        val dealNumberString = record._2._2.mkString("#")

        (record._1._1,record._1._2,record._1._3,deal_date,record._2._1.bigDecimal,dealNumberString)
      })
      .saveToPhoenix("ROOT_DEAL_AMOUNT_TRANS_INFO",Seq("MEMBER_ID","PARTICIPANT_TYPE",
        "BOND_TYPE_F","DEAL_DT","FULL_PRICE","DEAL_NUMBER"),conf,Some(ZOOKEEPER_URL))

    log.info("root_deal_amount_trans_info save success!")
    println("root_deal_amount_trans_info save success!")

    /*
    <买方机构ID,债券类型【4分类】，交易日期 >  <卖方机构ID，债券ID>
     */
    val buyer_day2 = bond_deal_table
      .map(x=>{Tuple2(Tuple3(x._2,x._7,x._6),Tuple2(Set(x._3),Set(x._13)))})
      .reduceByKey((x,y)=>Tuple2((x._1).union(y._1), (x._2) union (y._2))).cache()
    //交易对手覆盖度 需要复用

    /*
    <卖方机构ID,债券类型【4分类】，交易日期 >  <买方机构ID，债券ID>
    */
    val seller_day2 = bond_deal_table
      .map(x=>{Tuple2(Tuple3(x._3,x._7,x._6),Tuple2(Set(x._2),Set(x._13)))})
      .reduceByKey((x,y)=>Tuple2((x._1).union(y._1), (x._2) union (y._2))).cache()
    //交易对手覆盖度 需要复用

    /*
        <买方机构ID,参与者类型，债券类型【4分类】，交易日期> <卖方机构集合，交易债券ID>
         */
    val root_buyer_day2 = buyer_day2
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[Set[String],Set[String]])]()
        arrays += Tuple2(Tuple4 (root_member_id,"全部",record._1._2,record._1._3), record._2)
        if(isRT){
          arrays += Tuple2(Tuple4 (root_member_id,"机构",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_id,"产品",record._1._2,record._1._3), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2((x._1).union(y._1), (x._2) union (y._2)))

    /*
      <卖方机构ID,参与者类型，债券类型【4分类】，交易日期> <买方机构集合，交易债券ID>
    */
    val root_seller_day2 = seller_day2
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[Set[String],Set[String]])]()
        arrays += Tuple2(Tuple4 (root_member_id,"全部",record._1._2,record._1._3), record._2)
        if(isRT){
          arrays += Tuple2(Tuple4 (root_member_id,"机构",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_id,"产品",record._1._2,record._1._3), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2((x._1).union(y._1), (x._2) union (y._2)))

    /*
    <机构ID,参与者类型，债券类型【4分类】，交易日期> (<卖方机构集合，交易债券ID> <买方机构集合，交易债券ID>)
     */
    val root_counter_party_bond_info = (root_buyer_day2 union root_seller_day2)
      .reduceByKey((x,y)=>(x._1 union y._1, x._2 union y._2))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //        val deal_date = oracleDateFormat.parse(record._1._4)
        val deal_date = record._1._4
        val memberSetString = record._2._1.mkString("#")
        val bondSetString = record._2._2.mkString("#")

        (record._1._1,record._1._2,record._1._3,deal_date,memberSetString,bondSetString)
      })
      .saveToPhoenix("ROOT_COUNTER_PARTY_BOND_INFO",Seq("MEMBER_ID","PARTICIPANT_TYPE",
        "BOND_TYPE_F","DEAL_DT","COUNTER_ID_SET","BOND_ID_SET"),conf,Some(ZOOKEEPER_URL))

    log.info("root_counter_party_bond_info save success!")
    println("root_counter_party_bond_info save success!")


    //<机构ID,参与者类型，债券类型【4分类】，交易日期>  （交易对手集合，交易债券集合）
    val root_counter_party_number = (buyer_day2 union seller_day2)
      .reduceByKey((x,y)=>(x._1 union y._1, x._2 union y._2))
      .flatMap(record=>{
        //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[Set[String],Set[String]])]()
        arrays += Tuple2(Tuple4 (root_member_id,"全部",record._1._2,record._1._3), record._2)
        if(isRT){
          arrays += Tuple2(Tuple4 (root_member_id,"机构",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_id,"产品",record._1._2,record._1._3), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1 union y._1, x._2 union y._2))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._4)
        val deal_date = record._1._4
        val memberSetString = record._2._1.mkString("#")
        val bondSetString = record._2._2.mkString("#")

        (record._1._1,record._1._2,record._1._3,deal_date,memberSetString,bondSetString)
      })
      .saveToPhoenix("ROOT_COUNTER_PARTY_NUMBER",Seq("MEMBER_ID","PARTICIPANT_TYPE",
        "BOND_TYPE_F","DEAL_DT","COUNTER_ID_SET","BOND_ID_SET"),conf,Some(ZOOKEEPER_URL))

    log.info("root_counter_party_number save success!")
    println("root_counter_party_number save success!")

    /*
    <买方机构ID,债券类型【4分类】，交易日期，交易策略> <全价金额>
     */
    val buyer_day3 = bond_deal_table
      .map(x=>{Tuple2(Tuple4(x._2,x._7,x._6,x._4),x._10)})
      .reduceByKey((x,y)=>(x+y))

    /*
    <买方机构ID,参与者类型，债券类型【4分类】，交易日期,交易策略> <全价金额>
     */
    val root_buyer_day3 = buyer_day3
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple5 (root_member_id,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(isRT){
          arrays += Tuple2(Tuple5 (root_member_id,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_id,"产品",record._1._2,record._1._3,record._1._4), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>(x+y)).cache()

    /*
    <卖方机构ID,债券类型【4分类】，交易日期，交易策略> <全价金额>
     */
    val seller_day3 = bond_deal_table
      .map(x=>{Tuple2(Tuple4(x._3,x._7,x._6,x._4),x._10)})
      .reduceByKey((x,y)=>(x+y))


    /*
    <卖方机构ID,参与者类型，债券类型【4分类】，交易日期,交易策略> <全价金额>
     */
    val root_seller_day3 = seller_day3
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple5 (root_member_id,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(isRT){
          arrays += Tuple2(Tuple5 (root_member_id,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_id,"产品",record._1._2,record._1._3,record._1._4), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>(x+y)).cache()

    /*
   <机构ID,参与者类型，债券类型【4分类】，交易日期,交易策略> <全价金额【买入+卖出】>
   */
    val root_deal_strategy_info = (root_buyer_day3 union root_seller_day3)
      .reduceByKey((x,y)=>(x+y))
      .map(record=>{
        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._4)
        val deal_date = record._1._4
        (record._1._1,record._1._2,record._1._3,deal_date,record._1._5,record._2.bigDecimal)
      })
      .saveToPhoenix("ROOT_DEAL_STRATEGY_INFO",Seq("MEMBER_ID","PARTICIPANT_TYPE",
        "BOND_TYPE_F","DEAL_DT","DEAL_STRATEGY","FULL_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("root_deal_strategy_info save success!")
    println("root_deal_strategy_info save success!")

    /*
    <买方机构ID,债券类型【4分类】,交易日期，卖方机构类型> <成交金额，卖方机构ID集合>
     */
    val buyer_day4 = bond_deal_table
      .map(x=>{
        var sell_type_name = "None"
        var sell_id = x._3
        if(!broadMEMBER_D.value.contains(sell_id)){
          log.warn("broadMEMBER_D does not contain sell_id:"+sell_id)
          println("broadMEMBER_D does not contain sell_id:"+sell_id)
        }else{
          sell_type_name = broadMEMBER_D.value.get(sell_id).get._8
        }
        Tuple2(Tuple4(x._2,x._7,x._6,sell_type_name),Tuple2(x._10,Set(x._3)))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2).union(y._2)))

    /*
    <卖方机构ID,债券类型【4分类】,交易日期，买方机构类型> <成交量，买方机构ID集合>
     */
    val seller_day4 = bond_deal_table
      .map(x=>{
        val buy_type_name = broadMEMBER_D.value.get(x._2).get._8
        Tuple2(Tuple4(x._3,x._7,x._6,buy_type_name),Tuple2(x._10,Set(x._2)))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2).union(y._2)))


    /*
    <机构类型,债券类型【4分类】,交易日期，卖方机构类型> <成交金额，卖方机构ID集合>
     */
    val ctgry_counter_party_buyer = buyer_day4
      .map(record=>{
        val member_id = record._1._1 //得到机构ID
        var member_ins_show_nm = "None"

        if(!broadMemberID2Name.value.contains(member_id)){
          log.warn("broadMemberID2Name does not contain member_id:"+member_id)
          println("broadMemberID2Name does not contain member_id:"+member_id)
        }else{
          member_ins_show_nm = broadMemberID2Name.value.get(member_id).get
          //println("member_ins_show_nm : ",member_ins_show_nm)
        }
        //key: 机构类型，债券类型【8分类】,交易日期，卖方机构类型
        (Tuple4(member_ins_show_nm,record._1._2,record._1._3,record._1._4),record._2)
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2).union(y._2)))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._3)
        val deal_date = record._1._3
        val memberSetString = record._2._2.mkString("#")

        (record._1._1,record._1._2,deal_date,record._1._4,record._2._1.bigDecimal,memberSetString)

      })
      .saveToPhoenix("CTGRY_COUNTER_PARTY_BUYER",Seq("CTGRY_TYPE","BOND_TYPE_F","DEAL_DT","MEMBER_TYPE","FULL_PRICE","MEMBER_ID_SET"),conf,Some(ZOOKEEPER_URL))

    log.info("ctgry_counter_party_buyer save success!")
    println("ctgry_counter_party_buyer save success!")

    /*
   <买方机构ID,参与者类型，债券类型【4分类】，交易日期,卖方机构类型> <成交金额，卖方机构ID集合>
    */
    val root_counter_party_buyer = buyer_day4
      .flatMap(record=>{
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }

        //val root_member_id =  broadMemberID2RootMemberID.value.get(member_id).get //根据映射找出其父机构ID
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple2[BigDecimal,Set[String]])]()
        arrays += Tuple2(Tuple5 (root_member_id,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(isRT){
          arrays += Tuple2(Tuple5 (root_member_id,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_id,"产品",record._1._2,record._1._3,record._1._4), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2).union(y._2)))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._4)
        val deal_date = record._1._4
        val memberSetString = record._2._2.mkString("#")

        (record._1._1,record._1._2,record._1._3,deal_date,record._1._5,record._2._1.bigDecimal,memberSetString)
      })
      .saveToPhoenix("ROOT_COUNTER_PARTY_BUYER",Seq("BUYER_ID","PARTICIPANT_TYPE",
        "BOND_TYPE_F","DEAL_DT","MEMBER_TYPE","FULL_PRICE","MEMBER_ID_SET"),conf,Some(ZOOKEEPER_URL))


    log.info("root_counter_party_buyer save success!")
    println("root_counter_party_buyer save success!")

    //(买方机构ID,债券类型【4分类】,交易日期，买方机构类型)
    val counter_party_buyer = bond_deal_table
      .map(record=>{
        val member_id = record._2
        val rmember_id = record._3
        val deal_date = record._6
        val bond_type = record._7
        val member_type = broadMemberID2Name.value.get(member_id).get
        val rmember_type = broadMemberID2Name.value.get(rmember_id).get
        ((member_id,bond_type,deal_date,rmember_type,member_type),Set(rmember_id))
      }).reduceByKey((x,y)=>x++y)
      .map(x=>{
        val rmemberSetString = x._2.mkString("#")
        (x._1._1,x._1._2,x._1._3,x._1._4,x._1._5,rmemberSetString)
      }).saveToPhoenix("COUNTER_PARTY_BUYER",Seq("MEMBER_ID", "BOND_TYPE_F","DEAL_DT","RMEMBER_TYPE","MEMBER_TYPE","RMEMBER_ID_SET"),conf,Some(ZOOKEEPER_URL))


    log.info("counter_party_buyer save success!")
    println("counter_party_buyer save success!")


    //(卖方机构ID,债券类型【4分类】,交易日期，卖方机构类型)
    val counter_party_seller = bond_deal_table
      .map(record=>{
        val member_id = record._3
        val rmember_id = record._2
        val deal_date = record._6
        val bond_type = record._7
        val member_type = broadMemberID2Name.value.get(member_id).get
        val rmember_type = broadMemberID2Name.value.get(rmember_id).get

        ((member_id,bond_type,deal_date,rmember_type,member_type),Set(rmember_id))
      }).reduceByKey((x,y)=>x++y)
      .map(x=>{
        val rmemberSetString = x._2.mkString("#")
        (x._1._1,x._1._2,x._1._3,x._1._4,x._1._5,rmemberSetString)
      }).saveToPhoenix("COUNTER_PARTY_SELLER",Seq("MEMBER_ID", "BOND_TYPE_F","DEAL_DT","RMEMBER_TYPE","MEMBER_TYPE","RMEMBER_ID_SET"),conf,Some(ZOOKEEPER_URL))

    log.info("counter_party_seller save success!")
    println("counter_party_seller save success!")


    /*
    <机构类型,债券类型【4分类】,交易日期，买方机构类型> <成交金额，买方机构ID集合>
     */
    val ctgry_counter_party_seller = seller_day4
      .map(record=>{
        val member_id = record._1._1 //得到机构ID
        val member_ins_show_nm = broadMemberID2Name.value.get(member_id).get
        //key: 机构类型，债券类型【8分类】,交易日期，买方机构类型
        (Tuple4(member_ins_show_nm,record._1._2,record._1._3,record._1._4),record._2)
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2).union(y._2)))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._3)
        val deal_date = record._1._3
        val memberSetString = record._2._2.mkString("#")

        (record._1._1,record._1._2,deal_date,record._1._4,record._2._1.bigDecimal,memberSetString)
      })
      .saveToPhoenix("CTGRY_COUNTER_PARTY_SELLER",Seq("CTGRY_TYPE",
        "BOND_TYPE_F","DEAL_DT","MEMBER_TYPE","FULL_PRICE","MEMBER_ID_SET"),conf,Some(ZOOKEEPER_URL))

    log.info("ctgry_counter_party_seller save success!")
    println("ctgry_counter_party_seller save success!")

    /*
  <卖方机构ID,参与者类型，债券类型【4分类】，交易日期,买方机构类型> <成交金额，买方机构ID集合>
   */
    val root_counter_party_seller = seller_day4
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple2[BigDecimal,Set[String]])]()
        arrays += Tuple2(Tuple5 (root_member_id,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(isRT){
          arrays += Tuple2(Tuple5 (root_member_id,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_id,"产品",record._1._2,record._1._3,record._1._4), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2).union(y._2)))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._4)
        val deal_date = record._1._4
        val memberSetString = record._2._2.mkString("#")

        (record._1._1,record._1._2,record._1._3,deal_date,record._1._5,record._2._1.bigDecimal,memberSetString)
      })
      .saveToPhoenix("ROOT_COUNTER_PARTY_SELLER",Seq("SELLER_ID","PARTICIPANT_TYPE",
        "BOND_TYPE_F","DEAL_DT","MEMBER_TYPE","FULL_PRICE","MEMBER_ID_SET"),conf,Some(ZOOKEEPER_URL))

    log.info("root_counter_party_seller save success!")
    println("root_counter_party_seller save success!")

    /*
    净买入   每个机构对应到每个债券
    <买方机构ID,债券ID,交易日期，债券类型【4分类】，债券期限分类> <买入券面金额>
     */
    val buyer_day5 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple5(x._2,x._13,x._6,x._7,x._15),x._11)})
      .reduceByKey((x,y)=>(x+y))

    /*
      <买方机构ID,债券ID,子机构ID，参与者类型,交易日期，债券类型【4分类】，债券期限分类> <买入券面金额>
    */
    val root_buyer_day5 = buyer_day5
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val child_id = record._1._1
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple7[String,String,String,String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"全部",record._1._3,record._1._4,record._1._5), record._2)
        if(isRT){
          arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"机构",record._1._3,record._1._4,record._1._5), record._2)
        }else{
          arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"产品",record._1._3,record._1._4,record._1._5), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>(x+y))

    /*
       净卖出   每个机构对应到每个债券
       <卖方机构ID,债券ID,交易日期，债券类型【4分类】，债券期限分类> <卖出券面金额>
        */
    val seller_day5 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple5(x._3,x._13,x._6,x._7,x._15),x._11)})
      .reduceByKey((x,y)=>(x+y))


    /*
      <卖方机构ID,债券ID,参与者类型,交易日期，债券类型【4分类】，债券期限分类> <卖出券面金额>
    */
    val root_seller_day5 = seller_day5
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val child_id = record._1._1
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple7[String,String,String,String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"全部",record._1._3,record._1._4,record._1._5), record._2)
        if(isRT){
          arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"机构",record._1._3,record._1._4,record._1._5), record._2)
        }else{
          arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"产品",record._1._3,record._1._4,record._1._5), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>(x+y))

    /*
     <买方机构ID,债券ID,子机构ID,参与者类型,交易日期，债券类型【4分类】，债券期限分类> <买入券面金额>
     <卖方机构ID,债券ID,子机构ID，参与者类型,交易日期，债券类型【4分类】，债券期限分类> <卖出券面金额>
     <机构ID,债券ID,子机构ID,参与者类型,交易日期，债券类型【4分类】，债券期限分类> <买入券面金额,卖出券面金额>
     */
    val root_trade_deadline_info = (root_buyer_day5 fullOuterJoin  root_seller_day5)
      .map(x=>{
        var buy_full_price = scala.math.BigDecimal(0)
        var sell_full_price = scala.math.BigDecimal(0)

        if(x._2._1 != None)
          buy_full_price = x._2._1.get

        if(x._2._2 != None)
          sell_full_price = x._2._2.get

        ((x._1._1,x._1._2,x._1._3,x._1._4,x._1._5,x._1._6,x._1._7),(buy_full_price,sell_full_price))
      })
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._5)
        val deal_date = record._1._5

        (record._1._1,record._1._2,record._1._3,record._1._4,deal_date,record._1._6,record._1._7,record._2._1.bigDecimal,record._2._2.bigDecimal)
      })
      .saveToPhoenix("ROOT_TRADE_DEADLINE_INFO",Seq("MEMBER_ID","BOND_ID","CMEMBER_ID","PARTICIPANT_TYPE",
        "DEAL_DT","BOND_TYPE_F","YR_OF_TIME_TO_MRTY_TYPE","BUY_FULL_PRICE","SELL_FULL_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("root_trade_deadline_info save success!")
    println("root_trade_deadline_info save success!")

    /*
    <买方机构ID,债券ID,交易日期，债券类型【4分类】，债券期限分类> <买入券面金额>
    <机构类型,债券ID,交易日期，债券类型【4分类】，债券期限分类> <买入券面金额，卖出券面金额>
     */
    val ctgry_trade_deadline_info = (buyer_day5 fullOuterJoin  seller_day5)
      .map(x=>{
        var buy_full_price = scala.math.BigDecimal(0)
        var sell_full_price = scala.math.BigDecimal(0)

        if(x._2._1 != None)
          buy_full_price =  x._2._1.get

        if(x._2._2 != None)
          sell_full_price = x._2._2.get

        ((x._1._1,x._1._2,x._1._3,x._1._4,x._1._5),(buy_full_price,sell_full_price))
      })
      .map(record=>{
        val member_id = record._1._1 //得到机构ID
        val member_ins_show_nm = broadMemberID2Name.value.get(member_id).get

        (Tuple5(member_ins_show_nm,record._1._2,record._1._3,record._1._4,record._1._5),(record._2._1,record._2._2))
      }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._3)
        val deal_date = record._1._3

        (record._1._1,record._1._2,deal_date,record._1._4,record._1._5,record._2._1.bigDecimal,record._2._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_TRADE_DEADLINE_INFO",Seq("CTGRY_TYPE","BOND_ID",
        "DEAL_DT","BOND_TYPE_F","YR_OF_TIME_TO_MRTY_TYPE","BUY_FULL_PRICE","SELL_FULL_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("ctgry_trade_deadline_info save success!")
    println("ctgry_trade_deadline_info save success!")

    /*
     <买方机构ID,债券类型【8分类】,交易策略（数据源）,交易日期> <买入量【累和】，债券ID【集合】>
   */
    val buyer_day6 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple4(x._2,x._14,x._4,x._6),Tuple2(x._10,Set(x._13)))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2) union(y._2)))


    /*
    <买方机构ID,参与者类型，债券类型【8分类】,交易策略（数据源）,交易日期> <买入量【累和】，债券ID【集合】>
     */
    val root_buyer_day6 = buyer_day6
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple2[BigDecimal,Set[String]])]()
        arrays += Tuple2(Tuple5 (root_member_id,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(isRT){
          arrays += Tuple2(Tuple5 (root_member_id,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_id,"产品",record._1._2,record._1._3,record._1._4), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2) union(y._2))).cache()

    /*
      <卖方机构ID,债券类型【8分类】,交易策略（数据源）,交易日期> <卖出量【累和】，债券ID【集合】>

      买入量【累和】-------用于计算本机构在8种分类下的比例
     债券ID【集合】-------用于计算本机构交易债券数目
     */
    val seller_day6 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple4(x._3,x._14,x._4,x._6),Tuple2(x._10,Set(x._13)))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2) union(y._2)))


    /*
      (买方机构ID,交易策略（数据源）,交易日期)(债券ID集合)
       */
    val buy_ctgry_bond_set = bond_deal_table
      .map(x=>{
        Tuple2(Tuple3(x._2,x._4,x._6),Set(x._13))})
      .reduceByKey((x,y)=>(x union y)).cache()
    /*
      (卖方机构ID,交易策略（数据源）,交易日期)(债券ID集合)
       */
    val sell_ctgry_bond_set = bond_deal_table
      .map(x=>{
        Tuple2(Tuple3(x._3,x._4,x._6),Set(x._13))})
      .reduceByKey((x,y)=>(x union y)).cache()


    //(机构ID,数据源,交易日期)	(债券ID集合)
    val ctgry_bond_set = buy_ctgry_bond_set.union(sell_ctgry_bond_set)
      .reduceByKey((x,y)=>(x union y))
      .map(x=>{
        val member_type = broadMemberID2Name.value.get(x._1._1).get
        val bondSetString = x._2.mkString("#")
        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._3)

        (x._1._1,x._1._2,x._1._3,member_type,bondSetString)
      })
      .saveToPhoenix("CTGRY_BOND_SET",Seq("MEMBER_ID","DEAL_STRATEGY","DEAL_DT","MEMBER_TYPE","BOND_ID_SET"),conf,Some(ZOOKEEPER_URL))

    log.info("ctgry_bond_set save success!")
    println("ctgry_bond_set save success!")

    /*
    <卖方机构ID,参与者类型，债券类型【8分类】,交易策略（数据源）,交易日期> <卖出量【累和】，债券ID【集合】>
     */
    val root_seller_day6 = seller_day6
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple2[BigDecimal,Set[String]])]()
        arrays += Tuple2(Tuple5 (root_member_id,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(isRT){
          arrays += Tuple2(Tuple5 (root_member_id,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_id,"产品",record._1._2,record._1._3,record._1._4), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2) union(y._2))).cache()

    /*
    <买方机构ID,参与者类型，债券类型【8分类】,交易策略（数据源）,交易日期> <买入量【累和】，债券ID【集合】>
    <卖方机构ID,参与者类型，债券类型【8分类】,交易策略（数据源）,交易日期> <卖出量【累和】，债券ID【集合】>
    <机构ID	参与者类型	债券类型-8分类	数据源	交易日期>	<全价总额【买入+卖出】	债券ID集合【买入+卖出】>
     */
    val root_distribution_number = (root_buyer_day6 union root_seller_day6)
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, (x._2) union(y._2)))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._5)
        val deal_date = record._1._5
        val bondSetString = record._2._2.mkString("#")

        (record._1._1,record._1._2,record._1._3,record._1._4,deal_date,record._2._1.bigDecimal,bondSetString)
      })
      .saveToPhoenix("ROOT_DISTRIBUTION_NUMBER",Seq("MEMBER_ID","PARTICIPANT_TYPE",
        "BOND_TYPE_E","DEAL_STRATEGY","DEAL_DT","FULL_PRICE","BOND_ID_SET"),conf,Some(ZOOKEEPER_URL))

    log.info("root_distribution_number save success!")
    println("root_distribution_number save success!")

    /*
     <买方机构ID,债券类型【8分类】,交易策略（数据源）,交易日期,债券ID> <买入全价金额>
   */
    val buyer_day7 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple5(x._2,x._14,x._4,x._6,x._13),x._10)})
      .reduceByKey((x,y)=>(x+y)).cache()

    /*
     <卖方机构ID,债券类型【8分类】,交易策略（数据源）,交易日期,债券ID> <卖出全价金额>
   */
    val seller_day7 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple5(x._3,x._14,x._4,x._6,x._13),x._10)})
      .reduceByKey((x,y)=>(x+y)).cache()


    val root_bond_deal = (buyer_day7 union seller_day7)
      .reduceByKey((x,y)=>(x+y))
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple6[String,String,String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple6 (root_member_id,"全部",record._1._2,record._1._3,record._1._4,record._1._5), record._2)
        if(isRT){
          arrays += Tuple2(Tuple6 (root_member_id,"机构",record._1._2,record._1._3,record._1._4,record._1._5), record._2)
        }else{
          arrays += Tuple2(Tuple6 (root_member_id,"产品",record._1._2,record._1._3,record._1._4,record._1._5), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>(x+y))
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._5)
        val deal_date = record._1._5

        (record._1._1,record._1._2,record._1._3,record._1._4,deal_date,record._1._6,record._2.bigDecimal)
      })
      .saveToPhoenix("ROOT_BOND_DEAL",Seq("MEMBER_ID","PARTICIPANT_TYPE",
        "BOND_TYPE_E","DEAL_STRATEGY","DEAL_DT","BOND_ID","FULL_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("root_bond_deal save success!")
    println("root_bond_deal save success!")


    /*
    buyer_day7：<买方机构ID,债券类型【8分类】,交易策略（数据源）,交易日期,债券ID> <买入全价金额>
    seller_day7：<卖方机构ID,债券类型【8分类】,交易策略（数据源）,交易日期,债券ID> <卖出全价金额>
      <机构ID,债券ID,债券类型【8分类】,交易策略（数据源）,交易日期,机构类型>  <全价金额>
     */
    val ctgry_bond_deal = (buyer_day7 union seller_day7)
      .reduceByKey((x,y)=>(x+y))
      .map(x=>{
        val member_id = x._1._1
        val member_ins_show_nm = broadMemberID2Name.value.get(member_id).get

        Tuple2(Tuple6(member_id,x._1._5,x._1._2,x._1._3,x._1._4,member_ins_show_nm),x._2)})
      .map(record=>{

        //        val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //        val deal_date = oracleDateFormat.parse(record._1._5)
        val deal_date = record._1._5

        (record._1._1,record._1._2,record._1._3,record._1._4,deal_date,record._1._6,record._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_BOND_DEAL",Seq("MEMBER_ID","BOND_ID",
        "BOND_TYPE_E","DEAL_STRATEGY","DEAL_DT","MEMBER_TYPE","FULL_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("ctgry_bond_deal save success!")
    println("ctgry_bond_deal save success!")


    /*
      <买方机构ID,债券ID,交易日期,债券类型【8分类】> <买入全价金额,买入券面金额>
    */
    val buyer_day8 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple4(x._2,x._13,x._6,x._14),Tuple2(x._10,x._11))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2)).cache()

    /*
     <卖方机构ID,债券ID,交易日期,债券类型【8分类】> <卖出全价金额,卖出券面金额>
   */
    val seller_day8 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple4(x._3,x._13,x._6,x._14),Tuple2(x._10,x._11))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2)).cache()

    /*
    <买方机构ID,债券ID,交易日期,债券类型【8分类】> <买入全价金额,买入券面金额>
    <卖方机构ID,债券ID,交易日期,债券类型【8分类】> <卖出全价金额,卖出券面金额>
    <机构ID,债券ID，交易日期,债券类型【8分类】>  <（买入全价金额,买入券面金额），（卖出全价金额,卖出券面金额）>
     */

    val investment_returns = (buyer_day8 fullOuterJoin seller_day8)
      .map(x=>{
        var buy_full_price = scala.math.BigDecimal(0)
        var sell_full_price = scala.math.BigDecimal(0)
        var buy_face_price = scala.math.BigDecimal(0)
        var sell_face_price = scala.math.BigDecimal(0)

        if(x._2._1 != None){
          buy_full_price = x._2._1.get._1
          buy_face_price = x._2._1.get._2
        }

        if(x._2._2 != None) {
          sell_full_price = x._2._2.get._1
          sell_face_price = x._2._2.get._2
        }
        ((x._1._1,x._1._2,x._1._3,x._1._4),((buy_full_price,buy_face_price),(sell_full_price,sell_face_price)))
      })
      .cache()

    /*
    <机构ID,债券ID，交易日期，债券类型【8分类】>  <（买入全价金额,卖出全价金额,买入券面金额,卖出券面金额）>
    cache() root_investment_returns 及 ctgry_investment_returns复用
     */

    val investment_returns2 = investment_returns
      .map(x=>{
        Tuple2(Tuple4(x._1._1,x._1._2,x._1._3,x._1._4),Tuple4(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2))
      }).cache()


    /*
    <机构ID,债券ID，交易日期，债券类型【8分类】>  <买入全价金额,卖出全价金额,买入券面金额,卖出券面金额）\>
     */

    val base_investment_returns = investment_returns2
      .map(record=>{
        //        val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //        val deal_date = oracleDateFormat.parse(record._1._3)
        val deal_date = record._1._3

        (record._1._1,record._1._2,deal_date,record._1._4,record._2._1.bigDecimal,record._2._2.bigDecimal,record._2._3.bigDecimal,record._2._4.bigDecimal)
      })
      .saveToPhoenix("BASE_INVESTMENT_RETURNS",Seq("MEMBER_ID","BOND_ID",
        "DEAL_DT","BOND_TYPE_E","BUY_FULL_PRICE","SELL_FULL_PRICE","BUY_FACE_PRICE","SELL_FACE_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("base_investment_returns save success!")
    println("base_investment_returns save success!")


    /*
    <机构ID,参与者类型,债券ID，交易日期，债券类型【8分类】>  <（买入全价金额,卖出全价金额,买入券面金额,卖出券面金额）>
     */

    val root_investment_returns = investment_returns2
      .flatMap(record=>{
        //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple4[BigDecimal,BigDecimal,BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple5 (root_member_id,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(isRT){
          arrays += Tuple2(Tuple5 (root_member_id,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_id,"产品",record._1._2,record._1._3,record._1._4), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple4(x._1+y._1, x._2+y._2,x._3+y._3,x._4+y._4))
      .map(record=>{

        //        val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //        val deal_date = oracleDateFormat.parse(record._1._4)
        val deal_date = record._1._4

        (record._1._1,record._1._2,record._1._3,deal_date,record._1._5,record._2._1.bigDecimal,record._2._2.bigDecimal,record._2._3.bigDecimal,record._2._4.bigDecimal)
      })
      .saveToPhoenix("ROOT_INVESTMENT_RETURNS",Seq("MEMBER_ID","PARTICIPANT_TYPE",
        "BOND_ID","DEAL_DT","BOND_TYPE_E","BUY_FULL_PRICE","SELL_FULL_PRICE","BUY_FACE_PRICE","SELL_FACE_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("root_investment_returns save success!")
    println("root_investment_returns save success!")

    /*
      <机构类型,债券ID，交易日期，债券类型【8分类】>  <（买入全价金额,卖出全价金额,买入券面金额,卖出券面金额）>
    */

    val ctgry_investment_returns_saveBefore = investment_returns2
      .map(record=>{
        val member_id = record._1._1 //得到机构ID
        val member_ins_show_nm = broadMemberID2Name.value.get(member_id).get

        (Tuple4(member_ins_show_nm,record._1._2,record._1._3,record._1._4),record._2)
      })
      .reduceByKey((x,y)=>Tuple4(x._1+y._1, x._2+y._2,x._3+y._3,x._4+y._4)).cache()

    val ctgry_investment_returns = ctgry_investment_returns_saveBefore
      .map(record=>{

        //        val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //        val deal_date = oracleDateFormat.parse(record._1._3)
        val deal_date = record._1._3

        (record._1._1,record._1._2,deal_date,record._1._4,record._2._1.bigDecimal,record._2._2.bigDecimal,record._2._3.bigDecimal,record._2._4.bigDecimal)
      })
      .saveToPhoenix("CTGRY_INVESTMENT_RETURNS",Seq("CTGRY_TYPE","BOND_ID",
        "DEAL_DT","BOND_TYPE_E","BUY_FULL_PRICE","SELL_FULL_PRICE","BUY_FACE_PRICE","SELL_FACE_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("ctgry_investment_returns save success!")
    println("ctgry_investment_returns save success!")


    /*
    <买方机构ID,债券ID,交易日期，债券类型【4分类】> <买入全价金额,买入券面金额>
  */
    val buyer_day9 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple4(x._2,x._13,x._6,x._7),Tuple2(x._10,x._11))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2)).cache()

    /*
     <卖方机构ID,债券ID,交易日期，债券类型【4分类】> <卖出全价金额,卖出券面金额>
   */
    val seller_day9 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple4(x._3,x._13,x._6,x._7),Tuple2(x._10,x._11))})
      .reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2)).cache()


    /*
    < 机构ID,参与者类型,债券ID，交易日期，债券类型【4分类】>  <（买入全价金额,卖出全价金额,买入券面金额,卖出券面金额）>
     */
    val root_roi_performance = (buyer_day9.fullOuterJoin(seller_day9))
      .map(x=>{
        var buy_full_price = scala.math.BigDecimal(0)
        var sell_full_price = scala.math.BigDecimal(0)
        var buy_face_price = scala.math.BigDecimal(0)
        var sell_face_price = scala.math.BigDecimal(0)

        if(x._2._1 != None){
          buy_full_price = x._2._1.get._1
          buy_face_price = x._2._1.get._2
        }

        if(x._2._2 != None) {
          sell_full_price = x._2._2.get._1
          sell_face_price = x._2._2.get._2
        }
        ((x._1._1,x._1._2,x._1._3,x._1._4),(buy_full_price,sell_full_price,buy_face_price,sell_face_price))
      })
      .flatMap(record=>{
        //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple4[BigDecimal,BigDecimal,BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple5 (root_member_id,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(isRT){
          arrays += Tuple2(Tuple5 (root_member_id,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_id,"产品",record._1._2,record._1._3,record._1._4), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple4(x._1+y._1, x._2+y._2,x._3+y._3,x._4+y._4))
      .map(record=>{

        //        val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //        val deal_date = oracleDateFormat.parse(record._1._4)
        val deal_date = record._1._4

        (record._1._1,record._1._2,record._1._3,deal_date,record._1._5,record._2._1.bigDecimal,record._2._2.bigDecimal,record._2._3.bigDecimal,record._2._4.bigDecimal)
      })
      .saveToPhoenix("ROOT_ROI_PERFORMANCE",Seq("MEMBER_ID","PARTICIPANT_TYPE",
        "BOND_ID","DEAL_DT","BOND_TYPE_F","BUY_FULL_PRICE","SELL_FULL_PRICE","BUY_FACE_PRICE","SELL_FACE_PRICE"),conf,Some(ZOOKEEPER_URL))


    log.info("root_roi_performance save success!")
    println("root_roi_performance save success!")

    /*
    2.债券集中度
    */

    //<买方机构ID,债券ID，债券类型【4分类】,交易日期> <买入全价金额>

    val buyer_day10 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple4(x._2,x._13,x._7,x._6),x._10)})
      .reduceByKey((x,y)=>(x+y)).cache()


    //<卖方机构ID,债券ID，债券类型【4分类】,交易日期> <卖出全价金额>

    val seller_day10 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple4(x._3,x._13,x._7,x._6),x._10)})
      .reduceByKey((x,y)=>(x+y)).cache()

    /*
    <机构ID,参与者类型,债券ID，债券类型【4分类】,交易日期> <买入全价金额+卖出全价金额>
     */
    val root_bond_concentration = (buyer_day10 union seller_day10)
      .reduceByKey((x,y)=>(x+y))
      .flatMap(record=>{
        //将机构ID映射为父机构ID
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple5 (root_member_id,"全部",record._1._2,record._1._3,record._1._4), record._2)
        if(isRT){
          arrays += Tuple2(Tuple5 (root_member_id,"机构",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_id,"产品",record._1._2,record._1._3,record._1._4), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>(x+y))
      .map(record=>{

        //        val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //        val deal_date = oracleDateFormat.parse(record._1._5)
        val deal_date = record._1._5

        (record._1._1,record._1._2,record._1._3,record._1._4,deal_date,record._2.bigDecimal)
      })
      .saveToPhoenix("ROOT_BOND_CONCENTRATION",Seq("MEMBER_ID","PARTICIPANT_TYPE","BOND_ID","BOND_TYPE_F","DEAL_DT","FULL_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("root_bond_concentration save success!")
    println("root_bond_concentration save success!")


    /*
    4.净买入期限偏好
    */
    //1. 计算某机构对于每个债券一段时间内买入券面总额，卖出券面总额
    //   利用 buyer_day5 和 seller_day5 挑选出净买入和净卖出债券ID
    //2. 计算每个债券对应的加权平均年限

    //结构 (BOND_DEAL_ID, buy_id, sell_id, deal_strategy, deal_numbr, deal_dt, bond_type, instmt_crncy, trade_amnt, dirty_amnt, ttl_face_value, yr_of_time_to_mrty,bond_id,bond_type2)

    /*
    <买方机构ID,债券ID,交易日期，债券类型【4分类】，债券期限> <买入券面金额>
     */
    val buyer_day11 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple5(x._2,x._13,x._6,x._7,x._12),x._11)})
      .reduceByKey((x,y)=>(x+y)).cache()

    /*
      <买方机构ID,债券ID,子机构ID，参与者类型,交易日期，债券类型【4分类】，债券期限> <买入券面金额>
    */
    val root_buyer_day11 = buyer_day11
      .flatMap(record=>{
        val child_id = record._1._1
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple7[String,String,String,String,String,String,BigDecimal],BigDecimal)]()
        arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"全部",record._1._3,record._1._4,record._1._5), record._2)
        if(isRT){
          arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"机构",record._1._3,record._1._4,record._1._5), record._2)
        }else{
          arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"产品",record._1._3,record._1._4,record._1._5), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>(x+y))

    /*
    （卖出机构ID,债券ID,债券类型【4分类】,交易日期,债券期限）（卖出券面总额）
     */

    val seller_day11 = bond_deal_table
      .map(x=>{
        Tuple2(Tuple5(x._3,x._13,x._6,x._7,x._12),x._11)})
      .reduceByKey((x,y)=>(x+y)).cache()

    /*
      <卖方机构ID,债券ID,参与者类型,交易日期，债券类型【4分类】，债券期限分类> <卖出券面金额>
    */
    val root_seller_day11 = seller_day11
      .flatMap(record=>{
        val child_id = record._1._1
        val member_id = record._1._1
        val member_uniq = broadMEMBER_D.value.get(member_id).get._9
        var root_member_uniq = member_uniq
        var root_member_id = member_id

        var isRT = false

        if(!broadCIM_ORG_RL.value.contains(member_uniq)){
          isRT = true
        }
        else {
          root_member_uniq = broadCIM_ORG_RL.value.get(member_uniq).get
          root_member_id = broadMEMBER_UNIQ_TO_ID.value.get(root_member_uniq).get
        }
        val arrays = new ArrayBuffer[(Tuple7[String,String,String,String,String,String,BigDecimal],BigDecimal)]()
        arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"全部",record._1._3,record._1._4,record._1._5), record._2)
        if(isRT){
          arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"机构",record._1._3,record._1._4,record._1._5), record._2)
        }else{
          arrays += Tuple2(Tuple7 (root_member_id,record._1._2,child_id,"产品",record._1._3,record._1._4,record._1._5), record._2)
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>(x+y))

    /*
   (机构ID,债券ID,子机构ID,参与者类型，交易日期，债券类型【4分类】,债券期限）（买入券面总额，卖出券面总额）
    */
    val root_term_preference = (root_buyer_day11 fullOuterJoin  root_seller_day11)
      .map(x=>{
        var buy_full_price = scala.math.BigDecimal(0)
        var sell_full_price = scala.math.BigDecimal(0)

        if(x._2._1 != None)
          buy_full_price = x._2._1.get

        if(x._2._2 != None)
          sell_full_price = x._2._2.get

        ((x._1._1,x._1._2,x._1._3,x._1._4,x._1._5,x._1._6,x._1._7),(buy_full_price,sell_full_price))
      })
      .map(record=>{

        //val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //val deal_date = oracleDateFormat.parse(record._1._5)
        val deal_date = record._1._5

        (record._1._1,record._1._2,record._1._3,record._1._4,deal_date,record._1._6,record._1._7.bigDecimal,record._2._1.bigDecimal,record._2._2.bigDecimal)
      })
      .saveToPhoenix("ROOT_TERM_PREFERENCE",Seq("MEMBER_ID","BOND_ID","CMEMBER_ID","PARTICIPANT_TYPE","DEAL_DT"
        ,"BOND_TYPE_F","YR_OF_TIME_TO_MRTY","BUY_FULL_PRICE","SELL_FULL_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("root_term_preference save success!")
    println("root_term_preference save success!")


    //（买方机构类型,卖方机构类型，成交编号，全价金额，债券类型【4分类】，交易日期，交易策略）
    val ctgry_deal_strategy_info_pre = bond_deal_table
      .map(x=>{
        val buyer_id = x._2
        val buyer_ins_show_nm = broadMemberID2Name.value.get(buyer_id).get
        val seller_id = x._3
        val seller_ins_show_nm = broadMemberID2Name.value.get(seller_id).get
        val deal_numbr = x._5
        (buyer_ins_show_nm,seller_ins_show_nm,x._5,x._10,x._7,x._6,x._4)
      }).distinct()

    /*
    <买方机构类型,卖方机构类型，债券类型【4分类】，交易日期，交易策略> <全价金额>
     */
    val ctgry_deal_strategy_info = ctgry_deal_strategy_info_pre
      .map(x=>{
        Tuple2(Tuple5(x._1,x._2,x._5,x._6,x._7),x._4)
      })
      .reduceByKey((x,y)=>(x+y))
      .map(record=>{
        //        val oracleDateFormat = new SimpleDateFormat("yyyy-mm-dd")
        //        val deal_date = oracleDateFormat.parse(record._1._4)
        val deal_date = record._1._4
        (record._1._1,record._1._2,record._1._3,deal_date,record._1._5,record._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_DEAL_STRATEGY_INFO",Seq("CTGRY_TYPE_BUY","CTGRY_TYPE_SELL",
        "BOND_TYPE_F","DEAL_DT","DEAL_STRATEGY","FULL_PRICE"),conf,Some(ZOOKEEPER_URL))

    log.info("ctgry_deal_strategy_info save success!")
    println("ctgry_deal_strategy_info save success!")

    sc.stop()
  }
}