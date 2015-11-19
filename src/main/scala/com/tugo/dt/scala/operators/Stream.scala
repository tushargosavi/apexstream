package com.tugo.dt.scala.operators

import com.datatorrent.api.{DefaultInputPort, Sink}
import com.datatorrent.common.util.BaseOperator
import com.datatorrent.lib.algo.UniqueCounter
import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.serializers.JavaSerializer

@DefaultSerializer(classOf[JavaSerializer])
class MapO[A,B](func : A => B) extends BaseSinglePortOperator[A,B] with Serializable {
  override def processTuple(t: A) = {
    out.emit(func(t))
  }
}


@DefaultSerializer(classOf[JavaSerializer])
class FlatMap[A,B](func : A => Iterable[B]) extends BaseSinglePortOperator[A,B] with Serializable {
  override def processTuple(t: A) = {
    val lst = func(t)
    for (r <- lst) out.emit(r)
  }
}


@DefaultSerializer(classOf[JavaSerializer])
class Filter[A](func : A => Boolean) extends BaseSinglePortOperator[A,A] with Serializable {
  override def processTuple(t: A) = {
    if (func(t)) out.emit(t)
  }
}

@DefaultSerializer(classOf[JavaSerializer])
class Reduce[A,B](func : (A,B) => B, start : B) extends BaseSinglePortOperator[A,B] with Serializable {

  var acc : B = start

  override def processTuple(t: A) = {
    acc = func(t, acc)
  }

  override def beginWindow(windowId: Long): Unit = {
    acc = start
  }

  override def endWindow(): Unit = {
    out.emit(acc)
  }
}

@DefaultSerializer(classOf[JavaSerializer])
class UniqueCount[A] extends BaseSinglePortOperator[A, java.util.Map[A, Int]] with Serializable {

  @transient var counter : UniqueCounter[A] = null
  override def processTuple(t: A): Unit = counter.data.process(t)

  override protected def init(): Unit = {
    super.init()
    counter = new UniqueCounter[A]
    counter.count.setSink(new Sink[AnyRef] {
      override def put(t: AnyRef): Unit = out.emit(t.asInstanceOf[java.util.Map[A, Int]])
      override def getCount(b: Boolean): Int = 0
    })
  }

  override def beginWindow(windowId: Long): Unit = counter.beginWindow(windowId)
  override def endWindow(): Unit = counter.endWindow()

}

class ConsoleOutputOperator[A] extends BaseOperator {
  @transient
  val input = new DefaultInputPort[A] {
    override def process(t: A): Unit = println(t)
  }
}
