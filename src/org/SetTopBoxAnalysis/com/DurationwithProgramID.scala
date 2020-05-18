package org.SetTopBoxAnalysis.com

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object DurationwithProgramID {
  
  def main(args:Array[String]){
  
     System.setProperty("hadoop.home.dir", "C:/Users/Charan/Downloads/spark-2.4.5-bin-hadoop2.6")
      
    
  val spark=SparkSession
  .builder()
  .appName("DurationwithProgramID")
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
            var duration_value = 0
           var ProgramID=""
            val xmlValue = tokens(4).toString()
            
            val xml = XML.loadString(xmlValue)
     
            for (nv <- xml.child) {
            
               val Nv_Value=XML.loadString(nv.toString())
        val N_attribute=(Nv_Value\\"@n").toString()
        val V_attribute=(Nv_Value\\"@v").toString()
              //println("oneNVValue :"+oneNVValue)
              if((N_attribute == "DurationSecs"||N_attribute == "Duration") && V_attribute!="") {
                duration_value = Integer.parseInt(V_attribute)
                
              }
            
              if((N_attribute == "ProgramID") && V_attribute!="") {
                ProgramID = V_attribute
                
              }
            }
            
            (ProgramID,duration_value)  
   } }
    
   .groupByKey()
   
   result.foreach(println)
        spark.stop
  }
}