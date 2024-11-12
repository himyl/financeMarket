package mytest

import finance.marketing.distributed.utils.io
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FinanceMarketingModel")
    .setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder().getOrCreate()

    println("test starting --------")
    val dataPath_w="XXX\\data\\withcols.csv"
    val dataPath_wo = "XXX\\data\\feature.csv"
    val colPath="XXX\\data\\comma.txt"

    //test withCols
    //println("Test withCols ------------------:")
    //val df_w = io.getCsv(spark,"withCols",dataPath_w,"\t","\t")

    //df_w.show(20)

    //println("Test withoutCols ---------------------:")
    //val df_wo = io.getCsv(spark,"withoutCols",dataPath_wo,colPath,"\t","\t")
    //df_wo.show(20)
     println("Ok")

    val a=spark.createDataFrame(Seq(
      (1,2,3),
      (4,5,6),
      (1,7,9)
    )).toDF("x","y","z")

   val t:Array[(String,String)] = a.schema.fieldNames.map(s=>(s,"count"))
    val t1=Map(t:_*)
    //  a.agg(t1).show()
    val nn=a.schema.fieldNames
    a.agg(countDistinct(nn(0)),nn.drop(1).map(countDistinct(_)):_*).show()
    //a.agg(countDistinct("x"),countDistinct("y"),countDistinct("z")).show()

     spark.close()


  }
}
