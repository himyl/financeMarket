package finance.marketing.distributed.models

import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{Binarizer, Imputer, QuantileDiscretizer, VectorAssembler}
import org.apache.spark.ml.linalg.DenseVector
import scala.collection.immutable.Map
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator}
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel
import ml.dmlc.xgboost4j.scala.spark.TrackerConf

case class model_arc(pl: Pipeline, params: Array[ParamMap], eval: Evaluator)

object model_tvp_processor {

  def train_grid(train_data: DataFrame, alg: Pipeline, hyper_params: Array[ParamMap], eval: Evaluator, numFolds: Int = 2, numParals: Int = 3): PipelineModel = {

    val gridcv = new CrossValidator()
      .setEstimator(alg)
      .setEstimatorParamMaps(hyper_params)
      .setEvaluator(eval)
      .setNumFolds(numFolds)
      .setParallelism(numParals)

    val trained_model = gridcv.fit(train_data)

    return trained_model.bestModel.asInstanceOf[PipelineModel]

  }

  def train_split(train_data: DataFrame, alg: Pipeline, hyper_params: Array[ParamMap], eval: Evaluator, train_ratio: Double = 0.8, numParals: Int = 2): PipelineModel = {

    val train_valid_split = new TrainValidationSplit()
      .setEstimator(alg)
      .setEstimatorParamMaps(hyper_params)
      .setEvaluator(eval)
      .setTrainRatio(train_ratio)
      .setParallelism(numParals)

    val trained_model = train_valid_split.fit(train_data)

    return trained_model.bestModel.asInstanceOf[PipelineModel]

  }

  def valid {}

  def predict_2C(predict_data: DataFrame, alg: PipelineModel): DataFrame = {

    import predict_data.sparkSession.implicits._

    val prediction = alg.transform(predict_data).select("device_number", "probability")
      .rdd
      .map(s => (s.getAs[String]("device_number"), s.getAs[DenseVector]("probability")(1)))
      .toDF("device_number", "prob_1")

    return prediction
  }

  def predict() {}
}

object pipeline_generator {

  def classification_2C_V1(data: DataFrame): model_arc = {

    import data.sparkSession.implicits._

    //feature engineering

    val feature_name = data.columns.filterNot(s => s.equals("label"))
    val feature_stat = data.summary("80%")
    //data.stat.approxQuantile("sepalLength", Array(0.25, 0.5, 0.75), 0)

    val fc_distinct = data.agg(countDistinct(feature_name(0)).alias(feature_name(0)), feature_name.drop(1).map(x => countDistinct(x).alias(x)): _*) // count_distinct的情况
    val feature_cnt = fc_distinct.take(1)(0).toSeq.map(s => s.toString.toInt)
    val feature_name_cnt = feature_name.zip(feature_cnt)

    val feature_cate = feature_name_cnt.filter(_._2 < 10).map(_._1)
    val feature_cont = feature_name_cnt.filter(_._2 >= 10).map(_._1)

    val feature_pert = feature_stat.filter(col("summary") === "80%").take(1)(0).toSeq.drop(1).map(s => s.asInstanceOf[String].toDouble)
    val feature_name_pert = feature_name.zip(feature_pert)
    val feature_binary = feature_name_pert.filter(_._2 <= 0).map(_._1)
    val feature_kbin = feature_name_pert.filter(_._2 > 0).map(_._1)

    //null imputer
    val cate_imputer = new Imputer()
      .setInputCols(feature_cate)
      .setOutputCols(feature_cate.map(_ + "_impute"))
      .setStrategy("mode") //当前版本不支持众数mode 先用中位数代替 实际想用"mode"

    val cont_imputer = new Imputer()
      .setInputCols(feature_cont)
      .setOutputCols(feature_cont.map(_ + "_impute"))
      .setStrategy("median")

    //discretizer
    val binarizer = new Binarizer()
      .setInputCols(feature_binary.map(_ + "_impute"))
      .setOutputCols(feature_binary.map(_ + "_bin"))
      .setThreshold(0)

    val kbin = new QuantileDiscretizer()
      .setInputCols(feature_kbin.map(_ + "_impute"))
      .setOutputCols(feature_kbin.map(_ + "_bin"))
      .setNumBuckets(10)

    //vector assembler
    val vectorizer = new VectorAssembler()
      .setInputCols(cate_imputer.getOutputCols ++ cont_imputer.getOutputCols)
      .setOutputCol("feature")

    val gbdt = new GBTClassifier()
      .setFeaturesCol("feature")
      .setLabelCol("label")
      .setFeatureSubsetStrategy("auto")
      .setProbabilityCol("probability")
    //.setWeightCol("weight")

    val alg = new Pipeline()
      .setStages(Array(cate_imputer, cont_imputer, binarizer, kbin, vectorizer, gbdt))

    val grid_params = new ParamGridBuilder()
      .addGrid(gbdt.maxDepth, Array(6, 8))
      .addGrid(gbdt.maxIter, Array(100, 200, 500))
      .addGrid(gbdt.stepSize, Array(0.01, 0.05))
      .addGrid(gbdt.subsamplingRate, Array(0.5, 0.8))
      .build()

    //    val grid_params = new ParamGridBuilder()
    //      .addGrid(gbdt.maxDepth, Array(6))
    //      .addGrid(gbdt.maxIter, Array(100))
    //      .addGrid(gbdt.stepSize, Array(0.01))
    //      .addGrid(gbdt.subsamplingRate, Array(0.5))
    //      .build()

    val eval = new BinaryClassificationEvaluator()

    return model_arc(alg, grid_params, eval)

  }

  def classification_2C_V2(data: DataFrame): model_arc = {
    import data.sparkSession.implicits._

    //feature engineering
    val feature_name = data.columns.filterNot(s => s.equals("label"))

    //null imputer
    val data_imputer = new Imputer()
      .setInputCols(feature_name)
      .setOutputCols(feature_name.map(_ + "_impute"))
      .setStrategy("mean") //当前版本不支持众数mode 先用中位数代替 实际想用"mode"

    //vector assembler
    val vectorizer = new VectorAssembler()
      .setInputCols(data_imputer.getOutputCols)
      .setOutputCol("feature")

    val gbdt = new GBTClassifier()
      .setFeaturesCol("feature")
      .setLabelCol("label")
      .setFeatureSubsetStrategy("auto")
      .setProbabilityCol("probability")
    //.setWeightCol("weight")

    val alg = new Pipeline()
      .setStages(Array(data_imputer, vectorizer, gbdt))

    val grid_params = new ParamGridBuilder()
      .addGrid(gbdt.maxDepth, Array(6, 8))
      .addGrid(gbdt.maxIter, Array(100, 200, 500))
      .addGrid(gbdt.stepSize, Array(0.01, 0.05))
      .addGrid(gbdt.subsamplingRate, Array(0.5, 0.8))
      .build()
    //        val grid_params = new ParamGridBuilder()
    //          .addGrid(gbdt.maxDepth, Array(6,8))
    //          .addGrid(gbdt.maxIter, Array(100,200))
    //          .addGrid(gbdt.stepSize, Array(0.01))
    //          .addGrid(gbdt.subsamplingRate, Array(0.5))
    //          .build()


    val eval = new BinaryClassificationEvaluator()

    return model_arc(alg, grid_params, eval)

  }

  def classification_2C_V3(data: DataFrame): model_arc = {

    import data.sparkSession.implicits._

    //feature engineering

    val feature_name = data.columns.filterNot(s => s.equals("label"))
    val feature_stat = data.summary("80%")
    //data.stat.approxQuantile("sepalLength", Array(0.25, 0.5, 0.75), 0)

    val fc_distinct = data.agg(countDistinct(feature_name(0)).alias(feature_name(0)), feature_name.drop(1).map(x => countDistinct(x).alias(x)): _*) // count_distinct的情况
    val feature_cnt = fc_distinct.take(1)(0).toSeq.map(s => s.toString.toInt)
    val feature_name_cnt = feature_name.zip(feature_cnt)

    val feature_cate = feature_name_cnt.filter(_._2 < 10).map(_._1)
    val feature_cont = feature_name_cnt.filter(_._2 >= 10).map(_._1)

    val feature_pert = feature_stat.filter(col("summary") === "80%").take(1)(0).toSeq.drop(1).map(s => s.asInstanceOf[String].toDouble)
    val feature_name_pert = feature_name.zip(feature_pert)
    val feature_binary = feature_name_pert.filter(_._2 <= 0).map(_._1)
    val feature_kbin = feature_name_pert.filter(_._2 > 0).map(_._1)

    //null imputer
    val cate_imputer = new Imputer()
      .setInputCols(feature_cate)
      .setOutputCols(feature_cate.map(_ + "_impute"))
      .setStrategy("mode") //当前版本不支持众数mode 先用中位数代替 实际想用"mode"

    val cont_imputer = new Imputer()
      .setInputCols(feature_cont)
      .setOutputCols(feature_cont.map(_ + "_impute"))
      .setStrategy("median")

    //discretizer
    val binarizer = new Binarizer()
      .setInputCols(feature_binary.map(_ + "_impute"))
      .setOutputCols(feature_binary.map(_ + "_bin"))
      .setThreshold(0)

    val kbin = new QuantileDiscretizer()
      .setInputCols(feature_kbin.map(_ + "_impute"))
      .setOutputCols(feature_kbin.map(_ + "_bin"))
      .setNumBuckets(10)

    //vector assembler
    val vectorizer = new VectorAssembler()
      .setInputCols(cate_imputer.getOutputCols ++ cont_imputer.getOutputCols)
      .setOutputCol("feature")

//    val xgb = new XGBoostClassifier()
//      .setObjective("binary:logistic")
//      .setAllowNonZeroForMissing(true)
//      .setAlpha(1)
//      .setMinChildWeight(20)
//      .setFeaturesCol("feature")
//      .setLabelCol("label")


    val paramMap = Map(
      "tracker_conf" -> TrackerConf(0L, "python", "", "/opt/XXX/core/anaconda3/bin/python"))

    val xgb = new XGBoostClassifier(paramMap)
      .setObjective("binary:logistic")
      .setAllowNonZeroForMissing(true)
      .setAlpha(1)
      .setMinChildWeight(20)
      .setFeaturesCol("feature")
      .setLabelCol("label")

    val alg = new Pipeline()
      .setStages(Array(cate_imputer, cont_imputer, binarizer, kbin, vectorizer, xgb))

    val grid_params = new ParamGridBuilder()
      .addGrid(xgb.maxDepth,Array(6,8))
      .addGrid(xgb.numRound,Array(200,500))
      .addGrid(xgb.eta,Array(0.01,0.05))
      .addGrid(xgb.subsample,Array(0.5,0.8))
      .addGrid(xgb.colsampleBytree,Array(0.5,0.8))
      //.addGrid(xgb.colsampleBylevel,Array(0.5,0.8))
      //.addGrid(xgb.minChildWeight,Array(20,50))
      .build()

    val eval = new BinaryClassificationEvaluator()

    return model_arc(alg, grid_params, eval)

  }

  def classification_MC_V1 {}

  def regression_V1 {}

}


