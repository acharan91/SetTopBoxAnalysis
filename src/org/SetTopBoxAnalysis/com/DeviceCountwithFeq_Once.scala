package org.SetTopBoxAnalysis.com

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object DeviceCountwithFeq_Once {
   
  def main(args:Array[String]){
  
     System.setProperty("hadoop.home.dir", "C:/Users/Charan/Downloads/spark-2.4.5-bin-hadoop2.6")
      
    
  val spark=SparkSession
  .builder()
  .appName("DeviceCountwithFeq_Once")
  .master("local")
  .getOrCreate()
  
  val data=spark.read.textFile("C:/Users/Charan/Downloads/Set_Top_Box_Data.txt").rdd
  
 
   val result = data.filter {
          line => {
            val tokens = line.split("\\^")
            
            
            val evId = Integer.parseInt(tokens(2).toString())

            evId == 115 ||evId==118
         }
      }
    .map {
          line => {
            val tokens = line.split("\\^")
            var deviceId =tokens(5).toString()
           var Frequency=""
            val xmlValue = tokens(4).toString()
             
            val xml = XML.loadString(xmlValue)
     
            for (nv <- xml.child) {
            
               val Nv_Value=XML.loadString(nv.toString())
        val N_attribute=(Nv_Value\\"@n").toString()
        val V_attribute=(Nv_Value\\"@v").toString()
        
        if(N_attribute=="Frequency")
        {
          Frequency=V_attribute
        }
}
            (deviceId,Frequency)
          }}
    .filter{rec=>{
      
      rec._2=="Once"
    }}
    .map( rec => (rec._1, 1))
      .reduceByKey(_+_)
      .sortBy(rec => (rec._2), false)
   println("DeviceCountwithFeq_Once: "+result.count())
    spark.stop
  }
  }