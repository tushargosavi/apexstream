package com.tugo.dt.scala.dag

import com.datatorrent.api.Operator
import com.datatorrent.api.Operator.{OutputPort, InputPort}

import scala.collection.mutable

object PortMapper {
  def mapPorts(op : Operator): (mutable.Map[String, InputPort[_]], mutable.Map[String, OutputPort[_]]) = {
    val iports : mutable.Map[String, InputPort[_]] = new mutable.HashMap[String, InputPort[_]]()
    val oports : mutable.Map[String, OutputPort[_]] = new mutable.HashMap[String, OutputPort[_]]()
    print(op.getClass.getName + " " )
    var clazz : Class[_] = op.getClass
    while (clazz != classOf[Object]) {
      println(clazz)
      for(f <- clazz.getDeclaredFields) {
        println(f.getName + " " + f.getType)
        if (classOf[InputPort[_]].isAssignableFrom(f.getType)) {
          f.setAccessible(true)
          iports.put(f.getName, f.get(op).asInstanceOf[InputPort[_]])
        } else if (classOf[OutputPort[_]].isAssignableFrom(f.getType)) {
          f.setAccessible(true)
          oports.put(f.getName, f.get(op).asInstanceOf[OutputPort[_]])
        }
      }
      clazz = clazz.getSuperclass
    }
    (iports, oports)
  }
}
