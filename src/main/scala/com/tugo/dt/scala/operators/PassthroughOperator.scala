package com.tugo.dt.scala.operators

class PassthroughOperator[A] extends MergeOperator[A, A] {

  override def processTuple(t: A): Unit = out.emit(t)

}
