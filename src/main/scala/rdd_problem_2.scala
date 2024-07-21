import org.apache.spark.SparkContext

import org.apache.spark.SparkContext
object rdd_problem_2 {
  def main(args:Array[String]):Unit = {
    val sc = new SparkContext("local[*]","rdds")
//    2. You have an RDD containing web server logs with the following schema: (timestamp, ip_address,
//      url). You want to find the top 10 most frequently accessed URLs. How would you approach this
//    problem using RDD operations?
val ip = Array(("1-2-2014","120.1.2","xyz.com"),("asdfqwef","qdsfqef","xyz.com"),("adsf","asdf","xxy.com"))
    freq_ip(ip,sc)
  }
def freq_ip(ip:Array[(String,String,String)],sc:SparkContext): Unit = {
  val rdd1 = sc.parallelize(ip)
  val rdd2 = rdd1.map{case (_,_,url)=>(url,1)}
  val rdd3 = rdd2.reduceByKey((x,y)=>(x+y))
  rdd3.collect().foreach(println)




}
}
