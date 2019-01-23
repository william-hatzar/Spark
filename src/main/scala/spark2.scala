import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object spark2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf1=new SparkConf()
    conf1.setAppName("main")
    conf1.setMaster("local[*]")
    val sc=new SparkContext(conf1)
    val sqlContext = new SQLContext(sc)

    val rdd1 = sc.textFile("C:\\Users\\Admin\\Documents\\data.txt")
    val rdd2 = rdd1.subtract(sc.parallelize(Array(rdd1.first)))
    val rdd3 = rdd2.map(x=>{
      val record  = x.split("\t")
      (record(0).toInt,record(1).toInt,record(2).toInt,record(3).toInt)
    })

    val rdd4 = rdd3.map(x=>Row(x._1,x._2,x._3,x._4))

    val schema = StructType(
      StructField("user id", IntegerType, true) ::
        StructField("item id", IntegerType, true) ::
        StructField("rating", IntegerType, true) ::
        StructField("timestamp", IntegerType, true) ::
        Nil
    )

    val DF10 = sqlContext.createDataFrame(rdd4,schema)
    val DF11 = DF10.filter(DF10("rating")===5).groupBy("item id").count()
    val DF12 = DF11.orderBy(DF11("count").desc).show




  }




}
