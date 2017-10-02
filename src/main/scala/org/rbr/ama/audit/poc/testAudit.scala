package org.rbr.ama.audit.poc
import java.text.SimpleDateFormat


import org.json4s.JsonAST.JField
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object testAudit {
  
  def main(args: Array[String]) {
    
    val audit = "2017-09-28T22:20:42"
          
    
    val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val timeFormat = new SimpleDateFormat("HHmmss")
    
    val d = dateFormat.format(inputFormat.parse(audit)) 
    val t = timeFormat.format(inputFormat.parse(audit))
    
    println (d)
    println (t)
    
    
    
  }
  
  
  
}