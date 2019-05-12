package PubNativeTask


/**
  * Hello world!
  *
  */
object App extends App with SparkInstance {


  val dir = if (args.isEmpty) "data/" else args(0)
  val inputFiles = JSONETL.readJSON(dir) // Objective #1
  val eventsGroupped = ClicksImpressionsTransformer.calculateMetrics(inputFiles) // Objective #2
  val result = JSONETL.convertToJSON(eventsGroupped)
  JSONETL.write(result)


  /**
    *
    * ******************************************
    * Staring the use of Apache Spark - Objective #3
    * *******************************************
    *
    */

  val metrics = readJSON(dir + "output/metrics.json")
  val recommendations = metrics.transform(generateRecommendations)
  write(recommendations)



}
