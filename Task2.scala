package edu.gatech.cse6242
import java.io.IOException;
import java.io._;
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Task2 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))

    // read the file
    val file = sc.textFile("hdfs://localhost:8020" + args(0))
    val split_lines = file.map(_.split("\t"))
    // split_lines.collect().foreach(println)
     //split_lines.toArray().foreach(line => println(line))
	println("split")

	val a1 =split_lines.map(x=>(x(0),x(2).toInt*(-1)))
         val a=a1.filter(_._2 != -1)
        println("a")
       // a.collect().foreach(println)

	val b =split_lines.map(x=>(x(1),x(2).toInt)).filter(_._2 != 1)
	println("two maps")
       // b.collect().foreach(println)

  
    val list= a.union(b).reduceByKey(_ + _)
    println("final")
   // list.collect().foreach(println)
  val merged = list.groupBy ( _._1) .map { case (k,v) => k -> v.map(_._2).sum }
 
     println("merged")
 // merged.collect().foreach(println)
  
	val tabSeparated = merged.map(_.productIterator.mkString("\t"))
        println("tabseparated")
   //    tabSeparated.collect().foreach(println) 
   tabSeparated.saveAsTextFile("hdfs://localhost:8020" + args(1))
  }
}
