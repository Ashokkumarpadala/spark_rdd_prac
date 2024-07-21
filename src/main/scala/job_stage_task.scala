import org.apache.spark.SparkContext
//package main.job_stage_task
//package com.example.utils
object job_stage_task {
  def main(args: Array[String]): Unit = {
    rdd2(Array("as","sadf","fwefw"))
  }
  def rdd2(args:Array[String]):Unit = {
    val sc = new SparkContext("local[*]","spark_de")
val rdd = sc.parallelize(args)
    rdd.collect().foreach(println)
    rdd.getNumPartitions
  }
}
