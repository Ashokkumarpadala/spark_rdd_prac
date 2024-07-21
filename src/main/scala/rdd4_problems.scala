import org.apache.spark.SparkContext
object rdd4_problems {
  def main(args:Array[String]):Unit  ={
//    4. You have an RDD containing text data with the following schema: (document_id, text). You want
//    to find the top 10 most frequently occurring words across all documents. How would you
//    approach this problem using RDD operations?
val data1 = Array((1,"Ashok kumar is an DE "),(2,"Ashok kumar is an great engineer in india"))
    val sc = new SparkContext("local[*]","rdd4")
    doc_text_freq(data1,sc)
  }
  def doc_text_freq(data1:Array[(Int,String)],sc:SparkContext):Unit = {
    val rdd = sc.parallelize(data1)
    val rdd1 = rdd.flatMap { case (_, text) => text.split("\\s+")}
    val rdd2 = rdd1.map(x=>(x,1))
    val rdd3= rdd2.reduceByKey((x,y)=>(x+y))
    val rdd4 = rdd3.sortBy(x=>x._2,false)
    rdd4.take(10).foreach(println)
//    val rdd1 = rdd.flatMap((x,y)=>y.split(" "))

  }

}
