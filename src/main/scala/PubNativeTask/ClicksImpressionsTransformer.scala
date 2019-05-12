package PubNativeTask

import scala.collection.immutable.Map

object ClicksImpressionsTransformer {
  def calculateMetrics(allJSONFilesAsString: List[(String, String)]) = {
    val (clicksList, impressionsList) = convertClicksAndImpToScala(allJSONFilesAsString)
    val (clicksRenamed, impressions) = reduceAndRenameClicksAndImp(clicksList, impressionsList)
    groupAndJoinClicksAndImpressions(clicksRenamed, impressions)
  }

  private def convertClicksAndImpToScala(allJSONFilesAsString: List[(String, String)]) = {
    val clicksList = allJSONFilesAsString.map(clickFile => JSONETL.parseJSONintoList(clickFile._1))
    val impressionsList = allJSONFilesAsString.map(impressionFile => JSONETL.parseJSONintoList(impressionFile._2))
    (clicksList, impressionsList)
  }

  private def reduceAndRenameClicksAndImp(clicksList: List[List[Map[String, Object]]], impressionsList: List[List[Map[String, Object]]]) = {
    val clicks = clicksList.reduce(_ ++ _)
    val clicksRenamed = clicks.map(click => Map("id" -> click("impression_id"), "revenue" -> click("revenue"))) //renamed "impression_id" -> "id" to simplify grouping
    val impressions = impressionsList.reduce(_ ++ _)
    (clicksRenamed, impressions)
  }

  private def groupAndJoinClicksAndImpressions(clicks: List[Map[String, Object]], impressions: List[Map[String, Object]]) = {

    val impressionsGroupped = impressions.groupBy(key => key("id")).mapValues(maps => maps.head ++ Map("impressions" -> maps.size))
    val clicksGroupped = clicks.groupBy(key => key("id")).mapValues(list => Map("revenue" -> list.map(el => el("revenue").asInstanceOf[Double]).sum, "clicks" -> list.size))

    val impWithClicks = impressionsGroupped.toList ++ clicksGroupped.toList

    val impWithClicksGrouped = impWithClicks.groupBy(_._1).mapValues(_.map(_._2)) //Removing redundant indexes leftover from grouping
    impWithClicksGrouped.map(_._2.reduce(_ ++ _)) // Merging Maps into one
  }
}
