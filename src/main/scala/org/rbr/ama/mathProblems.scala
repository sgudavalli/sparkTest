package org.rbr.ama

object mathProblems {
   
  def isPrime ( x: Int) : Boolean = x match {
    case x if (x <= 1) =>  throw new NullPointerException
    case x if (x > 1) => isDivisible(x, x -1)
  }
  
  def isDivisible (x: Int, y: Int): Boolean = {
    
    ( (x%y), y) match {
      case (0, 1) => true
      case (0, y) => false
      case _ => isDivisible ( x, y -1 )
    }
    
  }
  
  def commonDivisor (x: Int, y: Int): Int = {
    
    if ( (isPrime (x)) | (isPrime (x)) )  1
    else if (x < y)
    {
          isCommonDivisorChk ( y, x, x)
    }
    else
    {
          isCommonDivisorChk ( x, y, y)
    }
    
  }
  
  def isCommonDivisorChk (x : Int, z: Int, y: Int) : Int = (x,z, y) match {
    
    case (x, z, 1) => 1
    case (x, z, y) => {
      if ( x% y == 0 & z%y == 0) 
      {
        y
      }
      else
      {
        isCommonDivisorChk(x, z, y-1)
      }
    }  
  }
  
  
  def main(args: Array[String]) {  
    println (  isPrime( 6700416) )
    
    println ( commonDivisor(1232, 224) )
    
  }
  
}


 