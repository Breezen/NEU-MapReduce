import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Main {
  // pre-process raw input string
  def parseInput(input: String, isTransform: Boolean) = {
    val tmp = input.split(",").map(i => i.toDouble)
    val neighbors = tmp.slice(0, tmp.length-1)
    val expanded = new ArrayBuffer[Double]()
    expanded.appendAll(neighbors)
    if (isTransform) {
      expanded.appendAll(DataTransform.mirroring(neighbors))
      var lastRotated = neighbors
      for (i <- 1 to 3) {
        val rotated = DataTransform.rotate90(lastRotated)
        expanded.appendAll(rotated)
        expanded.appendAll(DataTransform.mirroring(rotated))
        lastRotated = rotated
      }
    }
    LabeledPoint(tmp(tmp.length-1), Vectors.dense(expanded.toArray))
  }

  def main(args: Array[String]): Unit = {
    // args: 1. input path, 2. output path. 3. apply transform or not
    Logger.getLogger("org").setLevel(Level.ERROR)
    val trainPath = args(0)+"/train"
    val validationPath = args(0)+"/validate"
    val testPath = args(0)+"/test"

    var isTransform = false
      if (args.length > 2 && args(2).equals("y")) {
        isTransform = true
      }
    val spark = SparkSession
      .builder()
      .appName("BrainScan")
      .config("spark.network.timeout", "1000s")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val trainData = sc.textFile(trainPath).map(line => parseInput(line, isTransform))
    val validData = sc.textFile(validationPath).map(line => parseInput(line, isTransform))

    // training parameters
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 128
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 16
    val maxBins = 128

    val beforeTrain = System.nanoTime()
    val model = RandomForest.trainClassifier(trainData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val afterTrain = System.nanoTime()

    val beforeValidate = System.nanoTime()
    val labelAndPreds = validData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val afterValidate = System.nanoTime()

    val nValidData = validData.count()
    val nValidErr = labelAndPreds.filter(r => r._1 != r._2).count()
    val nTruePos = labelAndPreds.filter(r => r._1 == 1 && r._1 == 1).count()
    val nTrueNeg = labelAndPreds.filter(r => r._1 == 0 && r._1 == 0).count()
    val nFalsePos = labelAndPreds.filter(r => r._1 == 1 && r._1 == 0).count()
    val nFalseNeg = labelAndPreds.filter(r => r._1 == 0 && r._1 == 1).count()

    println("===== Effectiveness =====")
    println(s"# validation data = $nValidData")
    println(s"# validation error = $nValidErr")
    println(s"# true positive = $nTruePos")
    println(s"# true negative = $nTrueNeg")
    println(s"# false positive = $nFalsePos")
    println(s"# false negative = $nFalseNeg")
    println(s"Learned classification tree model:\n ${model.toDebugString}")

    println("===== Efficiency =====")
    println("training time = " + ((afterTrain - beforeTrain) / 1000000000) + "s")
    println("validation time = " + ((afterValidate - beforeValidate) / 1000000000) + "s")

    model.save(sc, args(1)+"/RFModel")
  }
}
