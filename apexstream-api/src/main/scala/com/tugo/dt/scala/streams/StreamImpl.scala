package com.tugo.dt.scala.streams

import com.datatorrent.api.Attribute.AttributeMap
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap
import com.datatorrent.api.DAG.Locality
import com.datatorrent.api.Operator.{InputPort, OutputPort}
import com.datatorrent.api.{Attribute, Operator}
import com.tugo.dt.scala.operators._

import scala.collection.mutable

case class PartitionInfo[T](num : Int, func : T => Int)

trait DeliverType[A]{
  def configure(source : Source[A], sink : Sink[A])
}

class Forward[A] extends DeliverType[A] {
  override def configure(source: Source[A], sink: Sink[A]): Unit = ???
}

class BroadCast[A] extends DeliverType[A] {
  override def configure(source: Source[A], sink: Sink[A]): Unit = ???
}

class Shuffle[A](part : A => Int = null) extends DeliverType[A] {
  override def configure(source: Source[A], sink: Sink[A]): Unit = ???
}

class StreamImpl[A](val ctx : Context, val source : Source[A], val locality: Locality = null, deliverType: DeliverType = null)
  extends Stream[A] {

  var sinks: mutable.MutableList[Sink[A]] = new mutable.MutableList()
  var attrMap: AttributeMap = new DefaultAttributeMap

  def STreamImpl() = {}

  override def map[B](func: A => B): Stream[B] = {
    addOperator[B](new MapO[A, B](func))
  }

  override def filter(func: A => Boolean): Stream[A] = {
    addOperator[A](new Filter[A](func))
  }

  override def flatMap[B](func: A => Iterable[B]): Stream[B] = {
    addOperator[B](new FlatMap[A, B](func))
  }

  override def reduce[B](func: (A, B) => B, start: B): Stream[B] = {
    addOperator[B](new Reduce[A, B](func, start))
  }

  override def count: Stream[Int] = ???

  override def print(): Unit = {
    addOperator(new ConsoleOutputOperator[A])
  }

  /** apply this stream codec on the next operator */
  override def partitionBy(func: (A) => Int): Stream[A] = ???

  override def addOperator[B](op: Operator, in: InputPort[A], out: OutputPort[B]): Stream[B] = {
    ctx.addOperator(op)
    sinks.+=(new Sink[A](in, null))
    if (out == null)
      null
    else
      new StreamImpl[B](ctx, new Source[B](op, out))
  }

  override def addOperator[B](op: Operator, port: OutputPort[B]): Stream[B] = {
    addOperator(op, getDefaultInputPort(op), port)
  }

  override def addOperator[B](op: Operator): Stream[B] = {
    addOperator[B](op, getDefaultInputPort(op), getDefaultOutputPort[B](op))
  }

  override def addSink(port: InputPort[A]): Stream[A] = {
    sinks.+=(new Sink(port, null))
    this
  }

  /** set the attribute on the operator */
  override def setAttribute[B](attr: Attribute[B], v: B): Stream[A] = {
    attrMap.put(attr, v)
    this
  }

  private def getDefaultSink(op: Operator): Sink[A] = {
    new Sink(ctx.getPortMapper(op).getInputPort[A], null)
  }

  private def getDefaultInputPort(op: Operator): InputPort[A] = {
    ctx.getPortMapper(op).getInputPort[A]
  }

  private def getDefaultOutputPort[B](op: Operator): OutputPort[B] = {
    ctx.getPortMapper(op).getOutputPort[B]
  }

  private def getDefaultSource[B](op : Operator) : Source[B] = {
    val port = ctx.getPortMapper(op).getOutputPort[B]
    if (port == null)
      null
    else
      new Source[B](op, port)
  }

  override def addTransform[B](func: (Stream[A]) => Stream[B]): Stream[B] = ???

  override def getSinks: Iterable[Sink[_]] = sinks

  override def getSource: Source[A] = source

  def init(): Unit = {
    if (this.source != null)
      ctx.register(this)
  }

  init()

  override def merge(joins: Stream[A]*): Stream[A] = {
    val op = new PassthroughOperator[A]()
    val newStream = addOperator[A](op, op.input, op.out)
    var i = 0
    /* extra ports are in arr for easy access */
    for(join <- joins) {
      join.addSink(op.arr(i))
      i+=1
    }
    newStream
  }

  override def th = new StreamImpl[A](ctx, source, Locality.THREAD_LOCAL)
  override def cl = new StreamImpl[A](ctx, source, Locality.CONTAINER_LOCAL)
  override def nl = new StreamImpl[A](ctx, source, Locality.NODE_LOCAL)
  override def rl = new StreamImpl[A](ctx, source, Locality.RACK_LOCAL)

  override def merge[B, C](other: Stream[B], func1: (A) => C, func2: (B) => C): Stream[C] = ???

  override def getLocality: Locality = locality

  def forward : Stream[A] = new StreamImpl[A](ctx, source, locality, new Forward[A])

  /** partition the stream based on given function */
  override def shuffle(func: (A) => Int): Stream[A] = {
    new StreamImpl[A](this.ctx, this.source, this.locality, new Shuffle[A](func))
  }

  override def broadcast() : Stream[A] = {
    new StreamImpl[A](this.ctx, this.source, this.locality, new BroadCast[A])
  }

  override def scale(num : Int) : Stream[A] = {
    ctx.setscale(source, num)
    this
  }

}
