package finance.marketing.distributed.models

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
import finance.marketing.distributed.utils.logSupport
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import finance.marketing.distributed.models._
import finance.marketing.distributed.utils._


abstract class alg_builder {

  def train(spark:SparkSession,trainData:DataFrame,modelPath:String) =  {}

  def predict(spark:SparkSession,predictData:DataFrame,modelPath:String,predictionPath:String)  = {}

  def valid(spark:SparkSession,predictData:DataFrame,modelPath:String) =  {}

}

object alg_wares {

  def get_alg(alg_version: String): alg_builder = {

    val useModel = alg_version match {

      case "alg_2C_V1" => alg_2C_V1
      case "alg_2C_V2" => alg_2C_V2
      case "alg_2C_V3" => alg_2C_V3

    }

    return useModel

  }
}


object alg_2C_V1 extends alg_builder {

  override def train(spark:SparkSession,trainData:DataFrame,modelPath:String): Unit = {

    val mc = pipeline_generator.classification_2C_V1(trainData)

    val alg = model_tvp_processor.train_grid(trainData,mc.pl,mc.params,mc.eval)

    io.save_model(alg,modelPath)
  }

  override def predict(spark:SparkSession,predictData:DataFrame,modelPath:String,predictionPath:String): Unit = {

    val alg = io.load_model(modelPath)

    val prediction = model_tvp_processor.predict_2C(predictData,alg)

    prediction.coalesce(10).write.mode(SaveMode.Overwrite).csv(predictionPath)

  }

  override def valid(spark:SparkSession,validData:DataFrame,modelPath:String) {}

  }

object alg_2C_V2 extends alg_builder {

  override def train(spark:SparkSession,trainData:DataFrame,modelPath:String): Unit = {

    val mc = pipeline_generator.classification_2C_V2(trainData)

    val alg = model_tvp_processor.train_grid(trainData,mc.pl,mc.params,mc.eval)

    io.save_model(alg,modelPath)
  }

  override def predict(spark:SparkSession,predictData:DataFrame,modelPath:String,predictionPath:String): Unit = {

    val alg = io.load_model(modelPath)

    val prediction = model_tvp_processor.predict_2C(predictData,alg)

    prediction.coalesce(10).write.mode(SaveMode.Overwrite).csv(predictionPath)

  }

  override def valid(spark:SparkSession,predictData:DataFrame,modelPath:String) {}

}

object alg_2C_V3 extends alg_builder {

  override def train(spark:SparkSession,trainData:DataFrame,modelPath:String): Unit = {

    val mc = pipeline_generator.classification_2C_V3(trainData)

    val alg = model_tvp_processor.train_grid(trainData,mc.pl,mc.params,mc.eval)

    io.save_model(alg,modelPath)
  }

  override def predict(spark:SparkSession,predictData:DataFrame,modelPath:String,predictionPath:String): Unit = {

    val alg = io.load_model(modelPath)

    val prediction = model_tvp_processor.predict_2C(predictData,alg)

    prediction.coalesce(10).write.mode(SaveMode.Overwrite).csv(predictionPath)

  }

  override def valid(spark:SparkSession,predictData:DataFrame,modelPath:String) {}

}