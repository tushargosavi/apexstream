package com.tugo.dt.scala.streams

import com.datatorrent.api.Operator.InputPort
import com.datatorrent.api.{Attribute, Operator}
import com.tugo.dt.scala.operators._

import scala.collection.mutable

class StreamImpl[A](val ctx : Context, val source : Source[A]) extends Stream[A] {

  var sinks : mutable.MutableList[Sink[A]] = new mutable.MutableList()

  def STreamImpl() = {}


  override def map[B](func : A => B): Stream[B] = {
    addOperator[B](new MapO[A,B](func))
  }

  override def filter(func : A => Boolean) : Stream[A] = {
    addOperator[A](new Filter[A](func))
  }

  override def flatMap[B](func : A => Iterable[B]) : Stream[B] = {
    addOperator[B](new FlatMap[A,B](func))
  }

  override def reduce[B](func : (A, B) => B, start : B) : Stream[B] = {
    addOperator[B](new Reduce[A,B](func, start))
  }

  override def count: Stream[Int] = ???


  override def print(): Unit = {
    addOperator(new ConsoleOutputOperator[A])
    null
  }

  /** apply this stream codec on the next operator */
  override def partitionBy(func: (A) => Int): Stream[A] = ???

  override def addOperator[B](op: Operator): Stream[B] = {
    ctx.addOperator(op)
    println("Adding sink " + getDefaultSink(op))
    sinks.+=(getDefaultSink(op))
    val source = getDefaultSource[B](op)
    if (source == null)
      null
    else
      new StreamImpl[B](ctx, source)
  }

  override def addSink(port: InputPort[A]): Stream[A] = {
    sinks.+=(new Sink(port, null))
    return this
  }

  /** set the property on the operator */
  override def setProperty[B](name: String, v: B): Stream[A] = ???

  /** set the attribute on the operator */
  override def setAttribute[B](attr: Attribute[B], v: B): Stream[A] = ???

  private def getDefaultSink(op : Operator) : Sink[A] = {
    new Sink(ctx.getPortMapper(op).getInputPort[A], null)
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
}
