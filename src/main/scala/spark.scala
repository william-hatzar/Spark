import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object spark {
    def main(args:Array[String]):Unit ={
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf1=new SparkConf()
      conf1.setAppName("main")
      conf1.setMaster("local[*]")
      val sc=new SparkContext(conf1)
      val sqlContext = new SQLContext(sc)

      val rdd1 = sc.textFile("C:\\Users\\Admin\\Documents\\companies.txt")
      val rdd2 = rdd1.subtract(sc.parallelize(Array(rdd1.first)))
      val rdd3 = rdd2.map(x=>{
        val record  = x.split(",")
        (record(0).toInt,record(1),record(2),record(3).toInt,record(4))
      })

      val rdd4 = rdd3.map(x=>Row(x._1,x._2,x._3,x._4,x._5))

      val schema = StructType(
        StructField("Regno", IntegerType, true) ::
          StructField("Name", StringType, true) ::
          StructField("Subject", StringType, true) ::
          StructField("Marks", IntegerType, true) ::
          StructField("Client", StringType, true) ::
          Nil
      )

      val DF10 = sqlContext.createDataFrame(rdd4,schema)
      DF10.show
      DF10.groupBy("Name").count.show

      DF10.groupBy("Name").agg(sum("Marks") as "Total Marks", mean("Marks") as "Average Marks", max("Marks") as ("Maximum Marks")).show
      DF10.orderBy("Marks").show
      DF10.orderBy(DF10("Marks").desc).show
      DF10.groupBy("Subject").agg(mean("Marks") as "Average Marks").show
    }
}
