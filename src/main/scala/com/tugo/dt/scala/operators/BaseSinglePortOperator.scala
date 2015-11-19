package com.tugo.dt.scala.operators

import java.io.ObjectInputStream

import com.datatorrent.api.{DefaultInputPort, DefaultOutputPort}
import com.datatorrent.common.util.BaseOperator
import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.serializers.JavaSerializer

@DefaultSerializer(classOf[JavaSerializer])
abstract class BaseSinglePortOperator[A,B] extends BaseOperator with Serializable {

  @transient
  var out : DefaultOutputPort[B] = null

  @transient
  var input : DefaultInputPort[A] = null

  protected def init() = {
    println("init of BaseSinglePort operator called")
    input = new DefaultInputPort[A] {
      override def process(t: A): Unit = {
        processTuple(t)
      }
    }
    out = new DefaultOutputPort[B]()
  }

  init()

  def processTuple(t:A)

  private def readObject(in : ObjectInputStream) = {
    in.defaultReadObject()
    init()
  }
}

