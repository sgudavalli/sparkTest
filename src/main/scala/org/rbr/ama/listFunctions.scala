package org.rbr.ama

object listFunctions {
  
  def last[A] (l: List[A] ): A= l match { 
    
    // when there is no tail => return head 
    case h :: Nil  => h 
    
    // when there is tail => loop and take off head one by one until there is no tail
    case _ :: t => last(t)
    
    // both above cases fail
    case _         => throw new NoSuchElementException
    
  }
  
  def penultimate [A]( l: List[A]) :A = l match {
    
    // when there is a head, an element and an empty List => return head 
    case h :: _ :: Nil  => h 
    
    // when there is a tail => loop and take off head until there is head :: one element :: Nil 
    case _ :: t => penultimate(t)
    
    // both above cases fail
    case _    => throw new NoSuchElementException    
    
  }
  
   def length  [A]( l : List[A]  ): Int = l match {
    
    // when the current list in recursion is empty 
    case Nil => 0 
    
    // when the current list in recursion contains tail
    case _ :: tail => 1+ length (tail)
    
  }
  
  def nthRecursive [A] ( n: Int, l : List[A] ) : A = (n,l) match {
    
    // when you are done recursion, n will be 0. at this point you can grab head 
    case (0, h :: _ ) => h
    
    // there is n available, which means you are not done recursion 
    case (n, _ :: t ) => nthRecursive ( n - 1, t)
    
    // if you request element at 5th place and the list contains only 4 elements then you throw error
    case (_, Nil) => throw new NoSuchElementException
    
  }
  
  def lastNthelement [A] (count: Int, result: List[A], current: List[A] ) : A = current match {
    
    // when there is pending to iterate and we have current as "empty"
    case Nil if count > 1 => throw new NoSuchElementException
    
    // when there is none pending to iterate and current as "empty"
    case Nil  => { 
      
      println (" i am on 2nd case statement")
      println(result.head) 
      result.head  
      } 
    
    // when there is pending to iterate; update tail until there is nothing to iterate from current 
    case _ :: tail => 
      {
         println ("i am on 3rd case statement")
         
         println("result -> " + result.toString)
         println("current -> " + current.toString) 
         println("tail -> " + tail.toString)
         println ("count -> " + count)
         lastNthelement ( count - 1, if (count > 0) result else result.tail, tail)
         
      }
     
  }
  
  def lastNthInitialize [A] (n:Int, l : List[A]) : A = { 
    
    if (n<0) throw new IllegalArgumentException
    else lastNthelement(n, l, l)
    
  }
  
  def reverse [A] (l : List[A]) : List[A] = {
    
    def reverseR [A] ( result: List[A], current: List[A]) : List[A] = current match { 
      
      
      case Nil => { 
        result }
      
      case h :: tail => reverseR(h :: result, tail)
      
      
    }
    reverseR (Nil, l)
    
  }

  // the List should read same in both directions
  def palindrome [A] (l: List[A]) : Boolean =  {
    
    def palindromeRecursive [A] (reverse : List[A], current: List[A], original: List[A]) : Boolean = current match 
    {
      case h :: Nil => {  if (h :: reverse == original) true else false }
      case h :: tail => palindromeRecursive (h :: reverse , tail, original)  
    }
    
    palindromeRecursive( Nil, l, l)
    
  }
  
  def flatten ( l: List[Any]) : List[Any] = l flatMap {
    
    // if element is a List 
    case ms : List[_] => {println ("ms => " + ms)
      flatten(ms) } 
    
    // if element is not a List
    case e => {println("e => " + e)
      List(e)}
    
  }
  
  
  def main(args: Array[String]) {
 
    
    println (  reverse ( List('A', 'B','D', 'X', 'A') ) )
    
     
    
     
    
  }
  
}


 