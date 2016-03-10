package com.tugo.dt.scala.streams

import com.datatorrent.api.Operator
import com.datatorrent.api.Operator.{OutputPort, InputPort}

import scala.collection.mutable
import scala.collection.immutable

trait PortMapper {
  def getInputPorts : Map[String, InputPort[_]]

  def getOutputPorts : Map[String, OutputPort[_]]

  def getInputPort[T](name : String) : InputPort[T]

  def getOutputPort[T](name : String) : OutputPort[T]

  def getInputPort[T] : InputPort[T]

  def getOutputPort[T] : OutputPort[T]

  def hasInputs : Boolean

  def hasOutputs : Boolean

  def getSource[T] : Source[T]

  def getSink[T] : Sink[T]
}

trait PortMappingFactory {
  def getPortMapper(op : Operator) : PortMapper
}

object DefaultPortMappingFactory extends PortMappingFactory {

  val cacheMappers : mutable.Map[Operator, PortMapper] = new mutable.HashMap[Operator, PortMapper]()
  override def getPortMapper(op: Operator): PortMapper = {
    if (cacheMappers.contains(op)) {
      return cacheMappers.getOrElse(op, null)
    } else {
      val mapper = new ReflexionPortMapper(op)
      cacheMappers.put(op, mapper)
      return mapper
    }
  }
}

class ReflexionPortMapper(val op : Operator) extends PortMapper {

  var inputs : immutable.Map[String, InputPort[_]] = null
  var outputs : immutable.Map[String, OutputPort[_]] = null

  def init(): Unit = {
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

    inputs = iports.toMap
    outputs = oports.toMap

  }

  override def getInputPorts: Map[String, InputPort[_]] = inputs

  override def getOutputPorts: Map[String, OutputPort[_]] = outputs

  override def getOutputPort[T](name: String): OutputPort[T] = outputs.getOrElse(name, null).asInstanceOf

  override def getOutputPort[T]: OutputPort[T] = {
    val entry = outputs.iterator.next
    if (entry != null)
      entry._2.asInstanceOf[OutputPort[T]]
    else
      null
  }

  override def getInputPort[T](name: String): InputPort[T] = inputs.getOrElse(name, null).asInstanceOf

  override def getInputPort[T]: InputPort[T] = {
    val entry = inputs.iterator.next
    if (entry != null)
      entry._2.asInstanceOf[InputPort[T]]
    else
      null
  }

  override def hasInputs: Boolean = inputs.nonEmpty

  override def hasOutputs: Boolean = outputs.nonEmpty


  init()

  override def getSource[T]: Source[T] = new Source[T](op, getOutputPort[T])

  override def getSink[T]: Sink[T] = new Sink[T](getInputPort[T], null)
}
