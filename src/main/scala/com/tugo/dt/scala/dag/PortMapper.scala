package com.tugo.dt.scala.dag

import com.datatorrent.api.Operator
import com.datatorrent.api.Operator.{OutputPort, InputPort}

import scala.collection.mutable

object PortMapper {
  def mapPorts(op : Operator): (mutable.Map[String, InputPort[_]], mutable.Map[String, OutputPort[_]]) = {
    val iports : mutable.Map[String, InputPort[_]] = new mutable.HashMap[String, InputPort[_]]()
    val oports : mutable.Map[String, OutputPort[_]] = new mutable.HashMap[String, OutputPort[_]]()
    var clazz : Class[_] = op.getClass
    while (clazz != classOf[Object]) {
      for(f <- clazz.getDeclaredFields) {
        if (classOf[InputPort[_]].isAssignableFrom(f.getType)) {
          f.setAccessible(true)
          println("found input port " + f.getName + " in " + clazz.getName)
          iports.put(f.getName, f.get(op).asInstanceOf[InputPort[_]])
        } else if (classOf[OutputPort[_]].isAssignableFrom(f.getType)) {
          println("found output port " + f.getName + " in " + clazz.getName)
          f.setAccessible(true)
          oports.put(f.getName, f.get(op).asInstanceOf[OutputPort[_]])
        }
      }
      clazz = clazz.getSuperclass
    }
    (iports, oports)
  }
}
