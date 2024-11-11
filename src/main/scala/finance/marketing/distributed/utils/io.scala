package finance.marketing.distributed.utils

import org.apache.spark
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, FloatType,StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties

import org.apache.spark.ml.PipelineModel

object io {

  // mode: 文件场景 withCols带表头文件 withoutCols无表头文件
  // dataPath: 数据文件地址
  // colPath: 表头文件地址
  // dataSep: 数据文件分隔符  默认","
  // colSep: 表头文件分隔符，default: ","
  // targetString: 特征中为StringType类型的Array

  def getCsv(spark: SparkSession,
             mode: String,
             dataPath: String,
             colPath: String = "",
             dataSep: String = "\t",
             colSep: String = "\t",
             targetCol: Array[String] = Array[String]()): DataFrame = {

    mode match {
      case "withCols" => {
        spark.read
          .option("inferSchema", "true")
          .option("sep", dataSep)
          .option("header", "true")
          .option("nanValue", "NULL")
          .csv(dataPath)
      }

      case _ => {
        val colArray =
          spark
            .sparkContext
            .textFile(colPath)
            .collect()(0)
            .split(colSep)

        val structArray = colArray.map {
          col =>
            if (targetCol.contains(col)) {
              StructField(col, StringType, true)
            } else {
              StructField(col, DoubleType,true)
            }
        }

        val dfSchema = StructType(structArray)

        spark.read.schema(dfSchema)
          .option("sep", dataSep)
          .option("header", "false")
          .option("nanValue", "NULL")
          .csv(dataPath)
      }
    }
  }

  def save_model(alg: PipelineModel, modelPath: String): Unit = {

    alg.write.overwrite().save(modelPath)

  }

  def load_model(modelPath: String): PipelineModel = {

    PipelineModel.load(modelPath)

  }

}



