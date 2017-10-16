package com.tugo.dt.scala.operators

import com.datatorrent.api.DefaultInputPort

import scala.collection.mutable

/**
  * A basic in memory join operator. It will perform join over a window of data.
  *
  * @param joinCond
  * @param join
  * @tparam A
  * @tparam B
  * @tparam C
  */
class JoinOperator[A, B, C](joinCond : (A, B) => Boolean, join : (A,B) => C)
  extends BaseSinglePortOperator[A, C] {

  val first = new mutable.MutableList[A]
  val snd = new mutable.MutableList[B]

  val input2 = new DefaultInputPort[B] {
    override def process(t: B): Unit = {
      for (b <- first) {
        if (joinCond(b, t))
          out.emit(join(b, t))
      }
      snd += t
    }
  }

  override def processTuple(t: A): Unit = {
    for (b <- snd) {
      if (joinCond(t, b))
        out.emit(join(t, b))
    }
    first += t
  }

  override def endWindow(): Unit = {
    first.clear()
    snd.clear()
  }

}
