package com.tugo.dt.scala.streams

import com.datatorrent.api.{Operator, Attribute}
import com.datatorrent.api.DAG.Locality
import com.datatorrent.api.Operator.{OutputPort, InputPort}

class StreamTerminatedException extends IllegalArgumentException

/** A end stream which throws exception on all the operations */
abstract class EndStreamImpl[A] extends Stream[A] {

  override def map[B](func: (A) => B): Stream[B] = throw new StreamTerminatedException

  override def reduce[B](func: (A, B) => B, start: B): Stream[B] = throw new StreamTerminatedException

  override def flatMap[B](func: (A) => Iterable[B]): Stream[B] = throw new StreamTerminatedException

  override def count: Stream[Int] = throw new StreamTerminatedException

  override def filter(func: (A) => Boolean): Stream[A] = throw new StreamTerminatedException

  /** apply this stream codec on the next operator */
  override def partitionBy(func: (A) => Int): Stream[A] = throw new StreamTerminatedException

  override def addOperator[B](op: Operator): Stream[B] = throw new StreamTerminatedException

  override def addOperator[B](op: Operator, port: OutputPort[B]): Stream[B] = throw new StreamTerminatedException

  override def addOperator[B](op: Operator, in: InputPort[A], out: OutputPort[B]): Stream[B] = throw new StreamTerminatedException

  override def addTransform[B](func: (Stream[A]) => Stream[B]): Stream[B] = throw new StreamTerminatedException

  override def getSinks: Iterable[Sink[_]] = throw new StreamTerminatedException

  override def merge[B, C](other: Stream[B], func1: (A) => C, func2: (B) => C): Stream[C] = throw new StreamTerminatedException

  /** set the attribute on the operator */
  override def setAttribute[B](attr: Attribute[B], v: B): Stream[A] = throw new StreamTerminatedException

  override def getSource: Source[A] = throw new StreamTerminatedException

  override def print(): Unit = throw new StreamTerminatedException

  override def addSink(port: InputPort[A]): Stream[A] = throw new StreamTerminatedException

  override def merge(joins: Stream[A]*): Stream[A] = throw new StreamTerminatedException

  override def forward(): Stream[A] = throw new StreamTerminatedException

  override def rl: Stream[A] = throw new StreamTerminatedException

  override def getLocality: Locality = throw new StreamTerminatedException

  override def nl: Stream[A] = throw new StreamTerminatedException

  override def cl: Stream[A] = throw new StreamTerminatedException

  override def th: Stream[A] = throw new StreamTerminatedException

}
