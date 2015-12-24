package com.tugo.dt.scala.dag

import com.datatorrent.api.Operator

class StreamSet[A,B](_sc : DTContext, prv : Stream[_], op : Operator, val s1 : Stream[A], val s2 : Stream[B])
  extends Stream[A](_sc, prv, op) {

  implicit def streamSettoStream(x : StreamSet[A,B]) : Stream[A] = x.s1

  def get[A](i : Int) : Stream[_] = {
    i match {
      case 0 => this.s1
      case 1 => this.s2
    }
  }

  def prev : Stream[_] = prv
}
