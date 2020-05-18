package org.SetTopBoxAnalysis.com
import scala.xml.XML
import org.apache.spark.sql.SparkSession

object DeviceCountOfChannelType {
  
  def main(args:Array[String])
  {
     System.setProperty("hadoop.home.dir", "C:/Users/Charan/Downloads/spark-2.4.5-bin-hadoop2.6")
      
    
    val spark=SparkSession
    .builder()
    .appName("DeviceCountOfChananelType")
    .master("local")
    .getOrCreate()
    
    val data=spark.read.textFile("C:/Users/Charan/Downloads/Set_Top_Box_Data.txt").rdd
   
    
    val result=data.filter{
      line=>{
        val tokens=line.split("\\^")
       val eventID=Integer.parseInt(tokens(2).toString())
       eventID==100
      }
    }
    
    .map{line=>{
      val tokens=line.split("\\^")
     val xmlValue=tokens(4).toString()
      val xml=XML.loadString(xmlValue)
      var channelType=""
      var device_ID=""
      for(nv<-xml.child)
      {
        val oneNVValue=XML.loadString(nv.toString())
        val oneN_Value=(oneNVValue\\"@n").toString()
        
        val OneV_Value=(oneNVValue\\"@v").toString()
       // println("Name: "+oneN_Value+"Value: "+OneV_Value)
        if(oneN_Value=="ChannelType"&& OneV_Value =="LiveTVMediaChannel")
        {
          
          device_ID=tokens(5).toString()
        }
      }
      
      (device_ID)
      
    }}
   println("Device Count for LiveTVMediaChannel: " +result.count())
    
    
  }
}