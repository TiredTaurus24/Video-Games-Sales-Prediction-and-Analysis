import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator

val spark = SparkSession.builder()
  .appName("SalesPrediction")
  .master("local")
  .getOrCreate()

val dataset = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("Downloads/archive(1)/vgsales.csv")

val columnsToDrop = Array("Rank","NA_Sales","EU_Sales","JP_Sales","Other_Sales")
val data = dataset.drop(columnsToDrop: _*)


val textColumns =Array("Platform","Name","Genre","Publisher")
val targetColumn = "Global_Sales"

val tokenizerStages: Array[Tokenizer] = textColumns.map { column =>
  new Tokenizer()
    .setInputCol(column)
    .setOutputCol(s"${column}_words")
}

val hashingTFStages: Array[HashingTF] = textColumns.map { column =>
  new HashingTF()
    .setInputCol(s"${column}_words")
    .setOutputCol(s"${column}_rawFeatures")
    .setNumFeatures(20000)
}

val idfStages: Array[IDF] = textColumns.map { column =>
  new IDF()
    .setInputCol(s"${column}_rawFeatures")
    .setOutputCol(s"${column}_idfFeatures")
}


val rf = new RandomForestRegressor()
  .setLabelCol(targetColumn)
  .setFeaturesCol("features")

val assembler = new VectorAssembler()
  .setInputCols(textColumns.map(c => s"${c}_idfFeatures"))
  .setOutputCol("features")


val pipeline = new Pipeline()
  .setStages(tokenizerStages ++ hashingTFStages ++ idfStages :+ assembler :+ rf)

val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

val model = pipeline.fit(trainingData)

val predictions = model.transform(testData)

predictions.select("prediction", targetColumn).show()

val evaluator = new RegressionEvaluator()
  .setLabelCol(targetColumn)
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error (RMSE): $rmse")
