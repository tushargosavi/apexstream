package com.tugo.dt.scala.operators

import java.io.ObjectInputStream

import com.datatorrent.api.Operator.InputPort
import com.datatorrent.api.annotation.{InputPortFieldAnnotation, OutputPortFieldAnnotation}
import com.datatorrent.api.{DefaultInputPort, DefaultOutputPort}
import com.datatorrent.common.util.BaseOperator
import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.serializers.JavaSerializer

@DefaultSerializer(classOf[JavaSerializer])
abstract class MergeOperator[A,B] extends BaseOperator with Serializable {

  var arr : List[InputPort[A]] = List()

  @transient
  @OutputPortFieldAnnotation(optional = true)
  var out : DefaultOutputPort[B] = null

  @transient
  @InputPortFieldAnnotation(optional = true)
  var input : DefaultInputPort[A] = null

  @transient
  @InputPortFieldAnnotation(optional = true)
  var input1 : DefaultInputPort[A] = null

  @transient
  @InputPortFieldAnnotation(optional = true)
  var input2 : DefaultInputPort[A] = null

  @transient
  @InputPortFieldAnnotation(optional = true)
  var input3 : DefaultInputPort[A] = null

  @transient
  @InputPortFieldAnnotation(optional = true)
  var input4 : DefaultInputPort[A] = null

  var totalInputs = 1

  protected def init() = {
    input = new DefaultInputPort[A] {
      override def process(t: A): Unit = {
        processTuple(t)
      }
    }
    input1 = new DefaultInputPort[A] {
      override def process(t: A): Unit = {
        processTuple(t)
      }
    }
    input2 = new DefaultInputPort[A] {
      override def process(t: A): Unit = {
        processTuple(t)
      }
    }
    input3 = new DefaultInputPort[A] {
      override def process(t: A): Unit = {
        processTuple(t)
      }
    }
    input4 = new DefaultInputPort[A] {
      override def process(t: A): Unit = {
        processTuple(t)
      }
    }
    out = new DefaultOutputPort[B]()
    arr = List(input1, input2, input3, input4)
  }

  def addInput() = totalInputs += 1

  init()

  def processTuple(t:A)

  private def readObject(in : ObjectInputStream) = {
    in.defaultReadObject()
    init()
  }
}

