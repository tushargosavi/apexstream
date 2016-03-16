package com.tugo.dt.scala.streams

import com.datatorrent.api.Context.PortContext
import com.datatorrent.api.Operator.OutputPort
import com.datatorrent.api.{DAG, Operator}
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.FileLineInputOperator
import com.datatorrent.stram.plan.logical.LogicalPlan
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

  def getOperatorName(op : Operator) = op.getClass.getSimpleName

  def build() = {
    /** Add all operators to the DAG */
    operators.foreach(op => {
      val name = getOperatorName(op) + "_" + count
      println("Adding operator " + name)
      dag.addOperator(name, op)
      count+=1
    })

    /** add streams */
    streams.filter(_.getSinks.nonEmpty).foreach((s) => {
      println("Adding stream s"+ count + " from " + s.getSource.op + " to ")
      val smeta = dag.addStream("s" + count)
      count+=1
      smeta.setSource(s.getSource.port)
      s.getSinks.foreach((sink) => {
        println("Adding stream s"+ count + " source " + s.getSource.op + " sink " + sink.port)
        smeta.addSink(sink.port)
        if (s.isParallel) {
          println("port is configured as parallel, setting parallel attribute on the port")
          dag.setInputPortAttribute[java.lang.Boolean](sink.port, PortContext.PARTITION_PARALLEL, true)
        }
      })
      if (s.getLocality != null) {
        println("setting locality of the stream " + s.getLocality)
        smeta.setLocality(s.getLocality)
      }
    })
  }

  def newInstance : DTContext = {
    val dag = new LogicalPlan()
    val conf = new Configuration()
    new DTContext(dag, conf)
  }
}

object DTContext {
  def newInstance : DTContext = {
    val dag = new LogicalPlan()
    val conf = new Configuration()
    new DTContext(dag, conf)
  }
}
