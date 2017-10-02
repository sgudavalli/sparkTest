/**
 * Created by sivagudavalli on 1/19/16.
 */

package org.rbr.ama.run

import org.json4s.JsonAST.JField
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class Child(name: String, age: Int, birthdate: Option[java.util.Date])
case class Address(street: String, city: String)
case class Person(name: String, address: Address, children: List[Child])

object Json4sTest {

  def main(arg: Array[String]): Unit = {

    implicit val formats = DefaultFormats

    val json = parse("""{ "name": "joe",
           "address": {
             "street": "Bulevard",
             "city": "Helsinki"
           },
           "children": [
             {
               "name": "Mary",
               "age": 5,
               "birthdate": "2004-09-04T18:06:22Z"
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }""")



    val must_ = json.extract[ Person ]

    println( must_.name )
    println( must_.address.city )
    println( must_.children(0).age )

  }

}
