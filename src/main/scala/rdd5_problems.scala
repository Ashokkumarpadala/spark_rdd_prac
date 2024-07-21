import org.apache.spark.SparkContext

object rdd5_problems {
  def main(args:Array[String]):Unit = {
//    5. You have an RDD containing sales data with the following schema: (product_id, sales_date,
//      sales_amount). You want to find the total sales for each product over a given time period. How
//    would you approach this problem using RDD operations?
val data2 = Array((1,"234134",500),(1,"1344",560),(2,"1325135",1000))
val sc = new SparkContext("local[*]","sad")
    sale_amount(data2 ,sc)
  }
def sale_amount(data2:Array[(Int,String,Int)],sc:SparkContext):Unit = {
  val rdd1 = sc.parallelize(data2)
  val rdd2 = rdd1.map{case (p_id,_,s_amount)=>(p_id,s_amount)}
  val rdd3 = rdd2.reduceByKey((x,y)=>(x+y))
  rdd3.collect().foreach(println)
}
}
