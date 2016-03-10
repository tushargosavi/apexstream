package com.tugo.dt.scala.streams

import com.datatorrent.api.Operator.OutputPort
import com.datatorrent.api.{DAG, Operator}
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.FileLineInputOperator
import org.apache.hadoop.conf.Configuration

trait Context {
  def addOperator(op : Operator)
  def register(stream : Stream[_])
  def getPortMapper(op : Operator) : PortMapper
}

class DTContext(val dag : DAG, val conf : Configuration) extends Context {

  val portMapperFactory : PortMappingFactory = DefaultPortMappingFactory

  val operators = new scala.collection.mutable.MutableList[Operator]()
  val streams = new  scala.collection.mutable.MutableList[Stream[_]]()
  var count : Int = 0

  override def addOperator(op: Operator): Unit = operators.+=(op)

  override def register(stream: Stream[_]): Unit = streams.+=(stream)

  override def getPortMapper(op: Operator) = portMapperFactory.getPortMapper(op)

  def getNextOperatorName : String = {
    count+=1
    "op_" + count.toString
  }

  def source[A](op : Operator, port : OutputPort[A]) : Stream[A] = {
    addOperator(op)
    new StreamImpl[A](this, new Source(op, port))
  }

  def fileInput(dir : String) : Stream[String] = {
    val op = new FileLineInputOperator()
    op.setDirectory(dir)
    source[String](op, op.output)
  }

  def build = {
    /** Add all operators to the DAG */
    operators.foreach(op => {
      println("Adding operator ")
      dag.addOperator("op_" + count, op)
      count+=1
    })

    /** add streams */
    streams.foreach((s) => {
      println("Adding stream s"+ count + " from " + s.getSource.op + " to ")
      val smeta = dag.addStream("s" + count)
      count+=1
      smeta.setSource(s.getSource.port)
      s.getSinks.foreach((sink) => {
        println("Adding stream s"+ count + " source " + s.getSource.op + " sink " + sink.port)
        smeta.addSink(sink.port)
      })
    })
  }
}
