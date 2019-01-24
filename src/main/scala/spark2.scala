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
      StructField("user_id", IntegerType, true) ::
        StructField("movie_id", IntegerType, true) ::
        StructField("rating", IntegerType, true) ::
        StructField("timestamp", IntegerType, true) ::
        Nil
    )

    val DF10 = sqlContext.createDataFrame(rdd4,schema)
    val DF11 = DF10.filter(DF10("rating")===5).groupBy("movie_id").count()
    val DF12 = DF11.orderBy(DF11("count").desc).show
    val DF13 = DF10.groupBy("movie_id").agg(mean("Rating")as "Average_Rating")
    val DF14 = DF13.orderBy(DF13("Average_Rating").desc).show


    val rdd13 = sc.textFile("C:\\Users\\Admin\\Documents\\movies.txt")
    val rdd14 = rdd13.map(x=>{
      val record = x.split("\\|")
      (record(0).toInt,record(1))
    })

    val rdd15 = rdd14.map(x=>Row(x._1,x._2))

    val schema1 = StructType(
      StructField("movie_id", IntegerType, true) ::
      StructField("Movie_Name", StringType, true) ::
      Nil
    )

    val DF20 = sqlContext.createDataFrame(rdd15,schema1)
    val DF21 = DF20.filter(DF20("movie_id")===50).show

    DF10.registerTempTable("Test_Table")
    DF20.registerTempTable("m")
    println("Showing results using SQL")
    val results = sqlContext.sql("SELECT user_id, movie_id  FROM Test_Table WHERE rating = 5 AND movie_id >500").show()

    def double(num1 :Int): Int = num1 * num1
    val udfDoubler = udf(double(_),LongType)
    sqlContext.udf.register("sqlDoubler",double(_))
    sqlContext.sql("SELECT FROM Test_Table WHERE double(a")


  }




}
