import Bz2WikiParser.parse
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

object PageRank {

	def main(args: Array[String]) {
		val spark = SparkSession.builder().appName("PageRankSpark").getOrCreate()
		val sc = spark.sparkContext
		val input = sc.textFile(args(0))
		val outputPath = args(1)
		val iters = if (args.length > 2) args(2).toInt else 10
		import spark.implicits._

		// [(pageName, [link])]
		val graph = input
			.map(parse)
			.filter(line => !line.equals("SKIP"))
			.map{ line =>
				val parts = line.split("~")
				val links = parts(1).substring(1, parts(1).length() - 1).split(", ").filter(link => !link.equals(""))
				(parts(0), links)
			}

		val nPage = graph.count()
		// [(pageName, score)]
		var scores = graph.mapValues(v => 1.0 / nPage)

		for (i <- 1 to iters) {
			// [(pageName, outScore)]
      		val contribs = graph.join(scores).values.flatMap{ case (links, score) =>
        		val size = links.size
        		links.map(link => (link, score / size))
      		}
      		scores = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    	}

    	val top100 = scores.takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
    	sc.parallelize(top100).coalesce(1).saveAsTextFile(outputPath)

		spark.stop()
	}
}