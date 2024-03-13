import org.apache.spark.{SparkContext,SparkConf}

// Create SparkSession
val sparkConf = new SparkConf().setAppName("video").setMaster("local")
val sc = new SparkContext(sparkConf)

val dataset = sc.textFile("Downloads/vgsales.csv")

val data1 = dataset.map(x => x.replace("#",""))

// remove special characters from the data like "",;,: etc.

val data2 = data1.map(x => x.replace("\"","")).map(x => x.replace(";","")).map(x => x.replace(":","")).map(x => x.replace("-","")).map(x => x.replace("_","")).map(x => x.replace("(","")).map(x => x.replace(")","")).map(x => x.replace("/","")).map(x => x.replace("\\","")).map(x => x.replace("'",""))
data2.collect()
val data = data2.filter(x => x.split(",")(3).matches("[0-9]+"))

//Sales distribution by year
val TotalplatformSales = data.map(x => x.split(",")).map(x => (x(3),x(10).toDouble)).reduceByKey(_+_)
TotalplatformSales.collect()

//Top 10 best-selling games
val topGames = data.map(x => x.split(",")).map(x => (x(1),x(10).toDouble)).sortBy(_._2, ascending = false).take(10)
topGames.foreach(println)

val topGames = data.map(x => x.split(",")).map(x => (x(1),x(10).toDouble)).sortBy(_._2, ascending = false).take(10)
topGames.foreach(println)

val topGames = data.map(x => x.split(",")).map(x => (x(1),x(10).toDouble)).sortBy(_._2, ascending = false).take(10)
topGames.foreach(println)

//Sales distribution by genre:
val genreSales = data.map(x => x.split(",")).map(x => (x(4),x(10).toDouble))
val genreTotalSales = genreSales.reduceByKey(_+_).sortBy(_._2, ascending = false)
genreTotalSales.foreach(println)

//Game releases per year
val Salesbyyear = data.map(x => x.split(",")).map(x => (x(3),1)).reduceByKey(_+_)
Salesbyyear.collect()

//Publisher with the highest sales
val publisherSales = data.map(x => x.split(",")).map(x => (x(5),x(10).toDouble))
val topPublisher = publisherSales.reduceByKey(_ + _).sortBy(_._2, ascending = false).take(1)
topPublisher.foreach(println)

//Platform distribution
val platformCount = data.map(x => x.split(",")).map(x => (x(2)  ,1)).countByKey().toSeq.sortBy(_._2).reverse
platformCount.foreach(println)

//Year with the highest sale
val yearSales = data.map(x => x.split(",")).map(x => (x(3),x(10).toDouble))
val yearTotalSales = yearSales.reduceByKey(_ + _)
val yearWithHighestSales = yearTotalSales.max()(Ordering.by(_._2))
println(s"Year with the highest sales: ${yearWithHighestSales._1}")

//Top 5 publishers
val publisherSales =data.map(x => x.split(",")).map(x => (x(5),x(10).toDouble))
val topPublishers = publisherSales.reduceByKey(_ + _).sortBy(_._2, ascending = false).take(5)
topPublishers.foreach(println)

//Genre distribution
val genreCount = data.map(x => x.split(",")).map(x => (x(4),1)).countByKey().toSeq.sortBy(_._2).reverse
genreCount.foreach(println)

//Most sold Genre by year
val yearGenreSales = data.map(x => x.split(","))
  .map(x => ((x(3), x(4)), x(10).toDouble))
  .reduceByKey(_ + _)

val mostSoldGenreByYear = yearGenreSales.map { case ((year, genre), sales) =>
  (year, (genre, sales))
}.reduceByKey { case ((genre1, sales1), (genre2, sales2)) =>
  if (sales1 > sales2) (genre1, sales1) else (genre2, sales2)
}.sortBy(_._1)

mostSoldGenreByYear.foreach { case (year, (genre, sales)) =>
  println(s"Most sold genre in $year: $genre (Sales: $sales)")
}

//Number of games published by publishers
val gamesPublishedByPublishersCount = data.map(x => x.split(","))
  .map(x => (x(5), 1))
  .reduceByKey(_ + _)
  .filter { case (_, count) => count < 50 }
  .map { case (publisher, count) => ("Other Publishers", count) }
  .reduceByKey(_ + _)

val gamesPublishedAboveThreshold = data.map(x => x.split(","))
  .map(x => (x(5), 1))
  .reduceByKey(_ + _)
  .filter { case (_, count) => count >= 50 }

val combinedResult = gamesPublishedByPublishersCount.union(gamesPublishedAboveThreshold)
  .sortBy(_._2, ascending = false)

combinedResult.foreach(println)
