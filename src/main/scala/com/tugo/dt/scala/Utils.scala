package com.tugo.dt.scala

import scala.collection.mutable.ListBuffer

object Utils extends Serializable {

  def mapToStrList[A,B](map : java.util.Map[A,B]): ListBuffer[String] = {
    val lst = new ListBuffer[String]
    for (a <- map.entrySet().toArray()) {
      lst.append(a.toString)
    }
    lst
  }

}
