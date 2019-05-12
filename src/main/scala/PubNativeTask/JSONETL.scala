package PubNativeTask

import java.io.{File, StringWriter}

import com.fasterxml.jackson.databind.node.ArrayNode

import scala.io.Source

object JSONETL extends Mapper {

  private lazy val writer = new StringWriter()

  def readJSON(inputDirectory: String) = {
    val clicks = readClicks(inputDirectory + "clicks")
    val impressions = readImpressions(inputDirectory + "impressions")

    (clicks,impressions)
  }

  def parseJSONintoList(stringJSON: String): List[Map[String, Object]] = mapper.readValue[List[Map[String, Object]]](stringJSON)

  def convertToJSON(result: Any) = {
    val root = mapper.createObjectNode()
    val jsonList: ArrayNode = mapper.valueToTree(result)
    root.putArray("list").addAll(jsonList)
  }

  def write(result: ArrayNode, path: String = "data/output/metrics.json") = {
    mapper.writeValue(new File(path), result)
  }

  private def readClicks(clicksInputDir: String) = {
    val files = getListOfFiles(clicksInputDir)
    files.map(getFileAsString(_))
  }

  private def readImpressions(impressionsInputDir: String) = {
    val files = getListOfFiles(impressionsInputDir)
    files.map(getFileAsString(_))
  }

  private def getFileAsString(file: File): String = Source.fromFile(file.getPath).getLines().mkString

  private def getListOfFiles(dirPath: String): List[File] = {
    val directory = new File(dirPath)
    if (directory.exists() && directory.isDirectory) {
      directory.listFiles().filter(_.isFile).toList
    } else List[File]()
  }

}
