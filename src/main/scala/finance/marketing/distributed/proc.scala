package finance.marketing.distributed

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, QuantileDiscretizer, StringIndexer, VectorAssembler, VectorIndexer}
import utils.logSupport
import org.apache.spark.sql.functions.{approxCountDistinct, approx_count_distinct, col, countDistinct}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import finance.marketing.distributed.models._
import finance.marketing.distributed.utils._
import finance.marketing.distributed.models._


object proc extends logSupport {

  // 特征路径
  private var feature1Path: String = _
  private var feature1ColPath: String = _

  private var feature2Path: String = _
  private var feature2ColPath: String = _

  private var labelPath: String = _
  private var labelColPath: String = _
  private var selectedFeaturePath: String = _
  // 模型路径
  private var modelPath: String = _
  // 处理类型
  private var procType: String = _
  // 模型类型
  private var algVersion: String = _
  // 预测数据表
  private var predictionPath: String = _

  //场景
  private var scene: String = _

  def parseArgs(args: Array[String]): Unit = {

    try {
      args.sliding(2, 2).foreach(x => {
        x(0) match {
          case "-feat1" => feature1Path = x(1)
          case "-feat1col" => feature1ColPath = x(1)
          case "-feat2" => feature2Path = x(1)
          case "-feat2col" => feature2ColPath = x(1)
          case "-labelPath" => labelPath = x(1)
          case "-labelColPath" => labelColPath = x(1)
          case "-selectedFeat" => selectedFeaturePath = x(1)
          case "-modelPath" => modelPath = x(1)
          case "-procType" => procType = x(1)
          case "-algVersion" => algVersion = x(1)
          case "-predictionPath" => predictionPath = x(1)
          case "-scene" => scene = x(1)
        }
      })
    } catch {
      case e: Exception =>
        log.error(
          """args 参数错误：
            |-feat1 特征1数据
            |-feat1col 特征1表头
            |-feat2 特征2数据
            |-feat2col 特征2表头
            |-labelPath  标签数据
            |-selectedFeat 入参特征
            |-modelPath 模型存储路径
            |-procType 训练、预测
            |-modelType 模型类型 rf,gbdt
            |-predTable 预测数据存储表
            |-scene 场景 connect,will
            |error:""".stripMargin, e)
        System.exit(1)
    }
  }

  def load_train_data(spark:SparkSession): DataFrame = {

    val feat1 = io.getCsv(spark,"withoutCols",feature1Path,feature1ColPath,"\t","\t",targetCol = Array("device_number"))
    val feat2 = io.getCsv(spark,"withoutCols",feature2Path,feature2ColPath,"\t","\t",targetCol = Array("device_number"))
    val labelInfo = io.getCsv(spark,"withoutCol",labelPath,labelColPath,"\t","\t",targetCol = Array("device_number"))
      .select("device_number","proc_date",scene+"_label").withColumnRenamed(scene+"_label","label")

    val data = labelInfo.join(feat1,Seq("device_number","proc_date"),joinType = "inner")
      .join(feat2,Seq("device_number","proc_date"),joinType = "inner")

//    val selectedFeatures = spark.read.csv(selectedFeaturePath).collect()
//      .map(s=>s.getAs[String](0))
//    val keep_cols = (selectedFeatures:+"label").map(s=>col(s))

    val target_data = data.drop("device_number","area_id","term_desc","os_desc","is_phone_contract","proc_date")
    //val target_data = data.select(keep_cols:_*)

    val feature_name = target_data.columns
    // agg的方式countDistinct
    // val fc_distinct = target_data.agg(countDistinct(feature_name(0)).alias(feature_name(0)), feature_name.drop(1).map(x=>countDistinct(x).alias(x)): _*) // count_distinct的情况
    // select的方式countDistinct
    //val fc_distinct = target_data.select(target_data.columns.map(c => countDistinct(col(c)).alias(c)): _*) // count_distinct的情况

    // agg的方式approx_count_distinct
    //val ap_dist = target_data.agg(approx_count_distinct(feature_name(0)), feature_name.drop(1).map(x=>countDistinct(x)): _*) // count_distinct的情况
    //val fc_distinct = target_data.select(target_data.columns.map(c => approx_count_distinct(col(c),0.05).alias(c)): _*)
    val fc_distinct = target_data.select(target_data.columns.map(c => approx_count_distinct(col(c)).alias(c)): _*)
    val feature_cnt = fc_distinct.take(1)(0).toSeq.map(s=>s.toString.toInt)
    val feature_name_cnt = feature_name.zip(feature_cnt)
    val drop_feature_name = feature_name_cnt.filter(_._2 <= 1).map(_._1)
    val train_data = target_data.drop(drop_feature_name:_*)

    return train_data
  }

  def load_valid_data(spark:SparkSession): DataFrame = {

    val feat1 = io.getCsv(spark,"withoutCols",feature1Path,feature1ColPath,"\t","\t",targetCol = Array("device_number"))
    val feat2 = io.getCsv(spark,"withoutCols",feature2Path,feature2ColPath,"\t","\t",targetCol = Array("device_number"))

    val valid_data = feat1.join(feat2,Seq("device_number","proc_date"),joinType = "inner")

//    val selectedFeatures = spark.read.csv(selectedFeaturePath).collect()
//      .map(s=>s.getAs[String](0)).map(s=>col(s))

    return valid_data
  }

  def load_predict_data(spark:SparkSession):DataFrame =  {

    val feat1 = io.getCsv(spark,"withoutCols",feature1Path,feature1ColPath,"\t","\t",targetCol = Array("device_number"))
    val feat2 = io.getCsv(spark,"withoutCols",feature2Path,feature2ColPath,"\t","\t",targetCol = Array("device_number"))

    val predict_data = feat1.join(feat2,Seq("device_number","proc_date"),joinType = "inner")

    return predict_data

  }

  def main(args:Array[String]): Unit = {

    val conf = new SparkConf()

    //.setMaster("local[2]")
    val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")
    val spark = SparkSession.builder()
      .config("spark.debug.maxToStringFields", "3000")
      .config("spark.memory.offHeap.enabled",true)
      .config("spark.memory.offHeap.size","16g")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    parseArgs(args)

//    val useModel = algVersion match {
//
//      case "alg_2C_V1" => alg_2C_V1
//      case "alg_2C_V2" => alg_2C_V2
//      case "alg_2C_V3" => alg_2C_V3
//
//    }

    val useModel = alg_wares.get_alg(algVersion)

    procType match {

      case "train" => {
        val trainData: DataFrame = load_train_data(spark)
        trainData.cache()
        useModel.train(spark, trainData, modelPath)
      }

      case "predict" => {
        val predictData: DataFrame = load_predict_data(spark)
        predictData.cache()
        useModel.predict(spark,predictData,modelPath,predictionPath)
      }

      case "valid" => {}
    }

    sc.stop()
  }

}
