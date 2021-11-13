package Question3
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.io.{BufferedWriter, FileWriter}

object question3 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("question3")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  def Array2Set(line: String):Set[Int] = {
    line.split(" ").map(strNum => strNum.toInt).toSet
  }

  def FilterFrequentItem(originalSet: RDD[Set[Int]], itemSet: List[List[Int]], threshold: Double) =
    {
      var oneMachineSet = originalSet.collect()
      var resList = new ListBuffer[List[Int]]
      for (subList <- itemSet){
        var count = 0
        var subSet = subList.toSet
        for (eachItemSet <- oneMachineSet){
          if (subSet.subsetOf(eachItemSet)){
            count += 1
          }
        }

        if (count > threshold)
          {
            resList.append(subList)
          }
      }
      resList.toList
    }

  def createNextItems(items: List[List[Int]]) = {
    var resList = new ListBuffer[List[Int]]
    for (i <- items.indices) {
      for (j <- i + 1 until items.size) {
        var firstList = items(i)
        var secondList = items(j)
        if (firstList.size == 1 || allSameButLast(firstList, secondList)) {
          val newItemList = (items(i) :+ items(j).last).sorted
          resList.append(newItemList)
        }
      }
    }
    resList.toList
  }

  def allSameButLast(firstList: List[Int], secondList: List[Int]):Boolean ={
    for (i <- 0 until firstList.size - 1) {
      if (firstList(i) != secondList(i))
        return false
    }
    if (firstList.last == secondList.last) {
      return false
    }
    true
  }

  def FPM(threshold: Double, itemRDD: RDD[String]):Unit ={
    val outPutPath = "Freq-"
    val trueThreshold = itemRDD.count() * threshold /100
    val setRDD = itemRDD.map(line => Array2Set(line))
    setRDD.persist()
    var k = 1
    var tempList = new ListBuffer[Int]
    var distinctRDD = setRDD.flatMap(line=>line).distinct()
    var arrayRes = distinctRDD.collect()
    val listSet = new ListBuffer[List[Int]]
    for (eachItem <- arrayRes) {
      listSet.append(List(eachItem))
    }
    var itemListSet = listSet.toList
    while (itemListSet.nonEmpty && k <= 10){
      val afterFileter = FilterFrequentItem(setRDD, itemListSet, trueThreshold)
      val file = outPutPath + k
      val writer = new BufferedWriter(new FileWriter(file))
      afterFileter.foreach(x => writer.write(x.toString()))
      writer.close()
      itemListSet = createNextItems(afterFileter)
      k += 1
    }
  }

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val iThreshold = args(1).toDouble
    val transStr = sc.textFile(inputPath)
    FPM(iThreshold, transStr)
    sc.stop()
  }

}
