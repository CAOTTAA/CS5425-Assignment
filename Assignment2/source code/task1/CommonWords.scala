package task1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.matching.Regex
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.io._
object CommonWords extends App{
  
  def clean(word: String): String={
  try {
        val pattern: Regex = "([A-Za-z]+)".r
        val pattern(res) = word
        return res.toLowerCase()
    } catch {
        case e: Exception => return ""
    }
  }
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
    
    val start: Long = System.currentTimeMillis
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    val stopWordsInput = sc.textFile("hdfs://localhost:9000/stopwords.txt")
    val stopWords = stopWordsInput.collect().toSet
    
    
    val input1 = sc.textFile("hdfs://localhost:9000/task1-input1.txt")
    val input1_counts = input1.flatMap(line => line.split(" "))
                 .map(word => (clean(word), 1)).filter(item => !stopWords.contains(item._1) && item._1.length()>1)
                 .reduceByKey(_ + _)
    val input2 = sc.textFile("hdfs://localhost:9000/task1-input2.txt")
    val input2_counts = input2.flatMap(line => line.split(" "))
                 .map(word => (clean(word), 1)).filter(item => !stopWords.contains(item._1) && item._1.length()>1)
                 .reduceByKey(_ + _)
    val joinedRDD = input1_counts.join(input2_counts)   
    
    val result = joinedRDD.mapValues(x => List(x._1, x._2).min).sortBy(_._2,false).take(15)
    
    printToFile(new File("result.txt")) { p =>
          result.foreach(p.println)
    }
    val end: Long = System.currentTimeMillis
    println(end-start)
}