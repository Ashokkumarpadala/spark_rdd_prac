import org.apache.spark.SparkContext
object rdd3_problems {
  def main(args:Array[String]):Unit = {
//    3. You have an RDD containing sensor data with the following schema: (sensor_id, timestamp,
//      value). You want to find the average value for each sensor over a given time period. How would
//    you approach this problem using RDD operations?
val sc = new SparkContext("local[*]","rdd3")
val data = Array((1,"23415",5),(1,"3524",48),(2,"12354",50))
    avgval_sensor(data,sc)
  }
def avgval_sensor(data:Array[(Int,String,Int)],sc:SparkContext):Unit = {
  val rdd1 = sc.parallelize(data)
  val rdd2 = rdd1.map{case (id,_,value)=> (id,value)}
  val rdd3=rdd2.reduceByKey((x,y)=>(x+y))
  rdd3.collect.foreach(println)
}
}
