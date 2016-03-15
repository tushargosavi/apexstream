package com.tugo.dt.scala.streams

import com.datatorrent.api.DAG.Locality
import com.datatorrent.api.Operator.{OutputPort, InputPort}
import com.datatorrent.api.{Attribute, Operator}

case class Source[A](val op : Operator, port : OutputPort[A])
case class Sink[A](port : InputPort[A], var locality : Locality)

/**
  * Represents one output port, when a function is applied to it, it will return
  * another stream and note down the operator used to generate the new stream.
  *
  * @tparam A
  */
trait Stream[A] {

  def map[B](func : A => B): Stream[B]

  def filter(func : A => Boolean) : Stream[A]

  def flatMap[B](func : A => Iterable[B]) : Stream[B]

  def reduce[B](func : (A, B) => B, start : B) : Stream[B]

  def merge(joins : Stream[A]*) : Stream[A]

  def merge[B, C](other : Stream[B], func1 : A => C, func2 : B => C) : Stream[C]

  def addOperator[B](op : Operator) : Stream[B]

  def addOperator[B](op : Operator, port : OutputPort[B]) : Stream[B]

  def addOperator[B](op : Operator, in : InputPort[A], out : OutputPort[B]) : Stream[B]

  def addSink(port : InputPort[A]) : Stream[A]

  def addTransform[B](func : (Stream[A] => Stream[B])) : Stream[B]

  def count : Stream[Int]

  def print()

  /** apply this stream codec on the next operator */
  def partitionBy(func : (A) => Int) : Stream[A]

  /** set the attribute on the operator */
  def setAttribute[B](attr: Attribute[B], v : B) : Stream[A]

  /** set the property on the operator */
  def setProperty(name : String, v : String) : Stream[A]

  def getSource : Source[A]

  def getSinks : Iterable[Sink[_]]

  def setLocality(locality: Locality) : Stream[A]

  def forward(flag : Boolean) : Stream[A]

}

/** When an operator is added to the stream, it may have multiple ports they are represented
  * as a stream set, it also gives Stream interface which wraps the first output port of the
  * operator for easy connectivity */
trait StreamSet[A] extends Stream[A] {
  /** get the stream for the perticular port name */
  def get[B](name : String) : Stream[B]
}
