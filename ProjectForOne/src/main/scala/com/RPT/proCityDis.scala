package com.RPT

import com.utils.{SchemaUtils, Utils2Type}
import org.apache.spark.sql.{Row, SparkSession}

object proCityDis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "D://spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()


    val values = spark.read.textFile("dir/originRes/*").rdd

    val rowRDD = values.map(t=>t.split(",",t.length)).filter(_.length >= 85)
      .map(arr=>{
        Row(
          arr(0),
          Utils2Type.toInt(arr(1)),
          Utils2Type.toInt(arr(2)),
          Utils2Type.toInt(arr(3)),
          Utils2Type.toInt(arr(4)),
          arr(5),
          arr(6),
          Utils2Type.toInt(arr(7)),
          Utils2Type.toInt(arr(8)),
          Utils2Type.toDouble(arr(9)),
          Utils2Type.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          Utils2Type.toInt(arr(17)),
          arr(18),
          arr(19),
          Utils2Type.toInt(arr(20)),
          Utils2Type.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          Utils2Type.toInt(arr(26)),
          arr(27),
          Utils2Type.toInt(arr(28)),
          arr(29),
          Utils2Type.toInt(arr(30)),
          Utils2Type.toInt(arr(31)),
          Utils2Type.toInt(arr(32)),
          arr(33),
          Utils2Type.toInt(arr(34)),
          Utils2Type.toInt(arr(35)),
          Utils2Type.toInt(arr(36)),
          arr(37),
          Utils2Type.toInt(arr(38)),
          Utils2Type.toInt(arr(39)),
          Utils2Type.toDouble(arr(40)),
          Utils2Type.toDouble(arr(41)),
          Utils2Type.toInt(arr(42)),
          arr(43),
          Utils2Type.toDouble(arr(44)),
          Utils2Type.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          Utils2Type.toInt(arr(57)),
          Utils2Type.toDouble(arr(58)),
          Utils2Type.toInt(arr(59)),
          Utils2Type.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          Utils2Type.toInt(arr(73)),
          Utils2Type.toDouble(arr(74)),
          Utils2Type.toDouble(arr(75)),
          Utils2Type.toDouble(arr(76)),
          Utils2Type.toDouble(arr(77)),
          Utils2Type.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          Utils2Type.toInt(arr(84))
        )
      })

    spark.createDataFrame(rowRDD,SchemaUtils.structtype).createOrReplaceTempView("allValues")

    spark.sql(
      """
        |select provincename,cityname,
        |      sum(OriginalRequest)  originalReques,
        |      sum(ValidRequest)  validRequest,
        |      sum(SuccessRequest)  successRequest,
        |      sum(ParticipateBid)  participateBid,
        |      sum(SuccessBid)/sum(ParticipateBid)  ParticiateSuccessed,
        |      sum(Show)  show,
        |      sum(Click)  click,
        |      sum(Click)/sum(Show) ClickPoint,
        |      sum(WinPrice)/1000 winprice,sum(adpayment)/1000 adpayment
        |      from
        |      (select provincename,cityname,
        |	  case when REQUESTMODE=1 and PROCESSNODE>=1 then 1 else 0 end OriginalRequest,
        |      case when REQUESTMODE=1 and PROCESSNODE>=2 then 1 else 0 end ValidRequest,
        |      case when REQUESTMODE=1 and PROCESSNODE>=3 then 1 else 0 end SuccessRequest,
        |      case when ISEFFECTIVE=1 and ISBILLING=1 and ISBID=1 then 1 else 0 end ParticipateBid,
        |      case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 then 1 else 0 end SuccessBid,
        |      case when REQUESTMODE=2 and ISEFFECTIVE=1 then 1 else 0 end Show,
        |      case when REQUESTMODE=3 and ISEFFECTIVE=1 then 1 else 0 end Click,
        |      case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 then 1 else 0 end adflag,
        |      WinPrice,adpayment
        |      from allValues) tmp group by provincename,cityname
      """.stripMargin
    ).show()
  }
}
