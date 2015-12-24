package com.tugo.dt.scala.dag

import com.datatorrent.api.Operator
import com.datatorrent.lib.stream.Counter
import com.tugo.dt.scala.operators._


class Stream[A](val _sc : DTContext, val prevStream : Stream[_], val op : Operator) {

  var next : Stream[_] = null

  def init() = {
    if (this.prevStream != null) {
      this.prevStream.next = this
    }
    _sc.addStream(prevStream, this)
  }

  def map[B](func : A => B): Stream[B] = {
    addOperator[B](new MapO[A,B](func))
  }

  def filter(func : A => Boolean) : Stream[A] = {
    addOperator[A](new Filter[A](func))
  }

  def flatMap[B](func : A => Iterable[B]) : Stream[B] = {
    addOperator[B](new FlatMap[A,B](func))
  }

  def reduce[B](func : (A, B) => B, start : B) : Stream[B] = {
   addOperator[B](new Reduce[A,B](func, start))
  }

  def uniqueCount : Stream[java.util.Map[A, Int]] = {
    addOperator[java.util.Map[A, Int]](new UniqueCount[A])
  }

  def addOperator[B](op : Operator) : Stream[B] = {
    new Stream[B](this._sc, this, op)
  }

  def count : Stream[Int] = {
    addOperator[Int](new Counter())
  }

  def print(): Stream[A] = {
    addOperator[A](new ConsoleOutputOperator[A]())
  }

  init
}

