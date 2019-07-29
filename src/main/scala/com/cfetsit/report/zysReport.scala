package com.cfetsit.report

import java.text.SimpleDateFormat
import java.util.Date

import com.cfetsit.util.csvTokenUtil
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.phoenix.spark._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, Set}

/* 输入参数：
    args[0]:zookeeperURL;
    args[1]=pd_d_path ;
    args[2] market_close_type_I;
    args[3]=member_ctgry_d;
    args[4]=after_hour_bond_type;
    args[5]=member_d_path ;
    ars[6]= MSTR_SLV_RL_TP_RL;
    args[7]= cim_ORG_RL;
    args[8]=trdx_deal_infrmn_rmv_d_path
    args[9]= bond_repo_deal_path;
    args[10]= bond_d;args[11]=ri_credit_rtng ;
    args[12]=dps_v_cr_dep_txn_dtl_data;
    args[13] = ev_cltrl_dtls
 */

object zysReport {

  @transient lazy val  log = Logger.getLogger(this.getClass)
  //@transient lazy val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf() //    //   sparkConf.setMaster("local[2]").setAppName("report")
    val sc = new SparkContext(sparkConf)

    val ZOOKEEPER_URL=args(0)      // val ZOOKEEPER_URL="127.0.0.1:2181"
    val conf = new Configuration()
    println("zookeeper url:"+args(0))
    log.info("zookeeper url:"+args(0))

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
    log.info("load market_close_type_i cost "+ (time3-time2)/1000+" seconds"+ " and read "+ market_close_type_i_data.size+" records")

    val broadMARKET_CLOSE_TYPE_ID_DATA = sc.broadcast(market_close_type_i_data)

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
      }else{
        ins_show_name = market_close_type_data.getOrElse(lineList.get(1),"None")
      }
      (lineList.get(0),ins_show_name)
    }).filter(x=> !x._2.equals("None")) //过滤掉找不到内部名称的机构
      .collectAsMap()
    val time4 = System.currentTimeMillis()
    log.info("load member_ctgry_d cost "+ (time4-time3)/1000+" seconds"+ " and read "+ member_ctgry_d_data.size+" records")

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
    log.info("load after_hours_bound_type cost "+ (time5-time4)/1000+" seconds"+" and read "+ after_hours_bound_type_data.size+" records")

    val broadAfterHoursBondType= sc.broadcast(after_hours_bound_type_data)
    val broadMEMER_CTGRY_D = sc.broadcast(member_ctgry_d_data)

    /*
    修改机构的member_d数据，增加机构类型名(可展示类型。) key(ip_id)机构ID
     */
    val member_d_rdd = sc.textFile(args(5))
      .filter(_.length!=0).filter(!_.contains("MEMBER")).filter(line=>{
      var flag = true
      val lineList = csvTokenUtil.split(line)
      if(lineList.size() != 10){ flag= false}
      else{
        for(i<-0 until lineList.size() if flag){
          if(lineList.get(i)==null || lineList.get(i).isEmpty){flag =false}
        }
        if(flag && !lineList.get(7).equals("9999-12-31")){
          flag = false
        }
      }
      flag
    })
      .map(line=>{
        val lineList = csvTokenUtil.split(line)
        val ip_id = lineList.get(0)
        val member_ctgry_id = lineList.get(1)
        val full_nm  = lineList.get(3)
        val oracleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val efctv_from_dt = oracleDateFormat.parse(lineList.get(6))
        val efctv_to_dt = oracleDateFormat.parse(lineList.get(7))
        val unq_id_in_src_sys  = lineList.get(8)
        val member_ctgry_d = broadMEMER_CTGRY_D.value
        var ins_show_name = "None"
        if(!member_ctgry_d.contains(member_ctgry_id)){
          log.warn("member_ctgry_d doesnot contain member_ctgry_nm "+member_ctgry_id)
        }else{
          ins_show_name = member_ctgry_d.getOrElse(member_ctgry_id,"None")
        }
        //质押式市场 机构展示名称
        var bond_after_show_name = "None"
        if(!broadAfterHoursBondType.value.contains(member_ctgry_id)){
          log.warn("broadAfterHoursBondType does not contain member_ctgry_id:"+member_ctgry_id)
        }else{
          bond_after_show_name = broadAfterHoursBondType.value.getOrElse(member_ctgry_id,"None")
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
    log.info("load member_d cost "+ (time6-time5)/1000+" seconds")


  }
}
