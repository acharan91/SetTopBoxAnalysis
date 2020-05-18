package org.SetTopBoxAnalysis.com

import org.apache.spark.sql.SparkSession
import scala.xml.XML
object GroupAllButtonNames {
  def main(args:Array[String])
  {
    val spark=SparkSession
    .builder()
    .appName("GroupAllButtonNames with DeviceID")
    .master("local")
    .getOrCreate()
    
    val data=spark.read.textFile("C:/Users/Charan/Downloads/Set_Top_Box_Data.txt").rdd
    
    val result=data.filter{line=>{
        val tokens=line.split("\\^")
        val eventID=tokens(2).toString().toInt
        eventID==107
    }}
    
    .map{line=>{
      val tokens=line.split("\\^")
      val xmlValue=tokens(4).toString()
      val xml=XML.loadString(xmlValue)
      val deviceID=tokens(5).toString()
      var ButtonName=""
      
      for(nv<-xml.child)
      {
        val Nv_Value=XML.loadString(nv.toString())
        val N_attribute=(Nv_Value\\"@n").toString()
        val V_attribute=(Nv_Value\\"@v").toString()
        
        if(N_attribute=="ButtonName")
        {
          ButtonName=V_attribute
        }
}
     (ButtonName,deviceID) 
    }}
      .groupByKey()
      .sortByKey(false)
      result.foreach(println)
      spark.stop
    }}