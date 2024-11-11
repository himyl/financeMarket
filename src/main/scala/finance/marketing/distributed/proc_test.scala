package finance.marketing.distributed

import finance.marketing.distributed.utils.io
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object proc_test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FinanceMarketingModel")
    //.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    //val spark = SparkSession.builder().getOrCreate()

    println("test starting --------")
    /*val dataPath_w="C:\\Users\\Administrator\\IdeaProjects\\chinaunicom_finance_marketing_distributed\\data\\withcols.csv"
    val dataPath_wo = "C:\\Users\\Administrator\\IdeaProjects\\chinaunicom_finance_marketing_distributed\\data\\feature.csv"
    val colPath="C:\\Users\\Administrator\\IdeaProjects\\chinaunicom_finance_marketing_distributed\\data\\comma.txt"
*/

    val dataPath_w = args(0)
    val dataPath_wo = args(1)
    val colPath = args(2)

    //test withCols
    println("Test withCols ------------------:")
    val df_w = io.getCsv(spark,"withCols",dataPath_w,"\t","\t", "",targetCol = Array(""))

    df_w.show(20)

    println("Test withoutCols ---------------------:")
    val df_wo = io.getCsv(spark,"withoutCols",dataPath_wo,colPath,"\t","\t",targetCol = Array("device_number","area_id","term_desc","os_desc","is_phone_contract","proc_date"))
    df_wo.show(20)

    spark.close()
  }
}
