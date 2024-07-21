import org.apache.spark.SparkContext
object Rdd_problems {
  def main(args:Array[String]):Unit = {
    val sc = new SparkContext("local[*]","rdds")
//    1. You have an RDD containing customer orders with the following schema: (order_id,
//      customer_id, order_date, order_total). You want to find the total revenue generated by each
//    customer. How would you approach this problem using RDD operations?
val records = Array((1,5,"2-1-2024",500),(2,5,"3-1-2024",1000))
    calc_total_amount_by_cust(records,sc)
  }
    def calc_total_amount_by_cust(records:Array[(Int,Int,String,Int)],sc:SparkContext):Unit= {
      val rdd = sc.parallelize(records)
      val rdd1 = rdd.map { case (_, customer_id, _, total_amount) => (customer_id, total_amount) }
      val rdd2 = rdd1.reduceByKey((x, y) => (x + y))
      rdd2.collect().foreach(println)
    }



}
