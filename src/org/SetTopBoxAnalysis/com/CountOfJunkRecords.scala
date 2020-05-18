package org.SetTopBoxAnalysis.com

import org.apache.spark.sql.SparkSession
import scala.xml.XML
object CountOfJunkRecords {
  
  
  def main(args:Array[String])
  {
    val spark=SparkSession
    .builder()
    .appName("CountOfJunkRecords")
    .master("local")
    .getOrCreate()
    
    val data=spark.read.textFile("C:/Users/Charan/Downloads/Set_Top_Box_Data.txt").rdd
    
    val result=data.filter{line=>{
        val tokens=line.split("\\^")
        val eventID=tokens(2).toString().toInt
        eventID==0
    }}
    
    .map{line=>{
      val tokens=line.split("\\^")
      val xmlValue=tokens(4).toString()
      val xml=XML.loadString(xmlValue)
      var junkRecords=""
      
      for(nv<-xml.child)
      {
        val Nv_Value=XML.loadString(nv.toString())
        val N_attribute=(Nv_Value\\"@n").toString()
        val V_attinute=(Nv_Value\\"@v").toString()
        if(N_attribute=="BadBlocks")
        {
          junkRecords=N_attribute
          
        }
       
      }
       (junkRecords,line)
    }}
    .filter(x => (x._1 =="BadBlocks"))
    
   println( "Count of Junk Record with BadBlocks: "+result.count())
  }
}