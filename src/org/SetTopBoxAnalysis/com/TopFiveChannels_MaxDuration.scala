package org.SetTopBoxAnalysis.com


import scala.xml.XML
import org.apache.spark.sql.SparkSession

object TopFiveChannels_MaxDuration {
  
  def main(args:Array[String]){
  
     System.setProperty("hadoop.home.dir", "C:/Users/Charan/Downloads/spark-2.4.5-bin-hadoop2.6")
      
    
  val spark=SparkSession
  .builder()
  .appName("MaxDurationAnalysis-TopFiveDevices")
  .master("local")
  .getOrCreate()
  
  val data=spark.read.textFile("C:/Users/Charan/Downloads/Set_Top_Box_Data.txt").rdd
  
 
   val result = data.filter {
          line => {
            val tokens = line.split("\\^")
            val eventId = Integer.parseInt(tokens(2).toString())

            eventId == 100
         }
      }
    .map {
          line => {
            val tokens = line.split("\\^")
            var duration_value = 0
           var ChannelType=""
            val xml_Value = tokens(4).toString()
            
            val xml = XML.loadString(xml_Value)
     
            for (nv <- xml.child) {
            
              val xml_NV = XML.loadString(nv.toString())
               
              val attribute_N = (xml_NV\\"@n").toString()
              
              val attribute_V = (xml_NV\\"@v")toString()
              
              //println("oneNVValue :"+oneNVValue)
              if((attribute_N == "DurationSecs"||attribute_N == "Duration") && attribute_V!="") {
                duration_value = Integer.parseInt(attribute_V)
                
              }
            
              if((attribute_N == "ChannelType") && attribute_V!="") {
                ChannelType = attribute_V
                
              }
            }
            
            (duration_value,ChannelType)  
   } }
    
   .sortByKey()
   .take(5)
   result.foreach(println)
  
      spark.stop
  }
}