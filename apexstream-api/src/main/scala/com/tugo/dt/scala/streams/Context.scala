package com.tugo.dt.scala.streams

import com.datatorrent.api.Context.{OperatorContext, PortContext}
import com.datatorrent.api.DAG.Locality
import com.datatorrent.api.Operator.{InputPort, OutputPort}
import com.datatorrent.api.{DAG, Operator}
import com.datatorrent.common.partitioner.StatelessPartitioner
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.FileLineInputOperator
import com.datatorrent.stram.plan.logical.LogicalPlan
import com.tugo.dt.scala.operators.StreamCodec
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable

trait Context {
  def setscale[A](source: Source[A], num: Int)

  def addOperator(op : Operator)
  def register(stream : Stream[_])
  def getPortMapper(op : Operator) : PortMapper
}

class DTContext(val dag : DAG, val conf : Configuration) extends Context {

  abstract case class DTStream(in : InputPort[_], outs : OutputPort[_], locality : Locality) {
    def setSource(s : Source[_])
    def addSink(sink : Sink[_])
  }

  val portMapperFactory : PortMappingFactory = DefaultPortMappingFactory

  val operators = new scala.collection.mutable.MutableList[Operator]()
  val streams = new  scala.collection.mutable.MutableList[Stream[_]]()
  val opScaleMap = new mutable.HashMap[Operator, Int]()
  var count : Int = 0
  var finalStreamMap : mutable.HashMap[Source, Seq[Sink[_]]] = new mutable.HashMap[Source, Seq[Sink[_]]]()

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

    // TODO get a map from source to sinks
    streams.filter(_.getSinks.nonEmpty).foreach((s) => {
      finalStreamMap(s.getSource, s.getSinks)
    }

    /** add streams */
    streams.filter(_.getSinks.nonEmpty).foreach((s) => {
      println("Adding stream s"+ count + " from " + s.getSource.op + " to ")
      val smeta = dag.addStream("s" + count)
      count+=1
      smeta.setSource(s.getSource.port)
      s.getSinks.foreach((sink) => {
        println("Adding stream s"+ count + " source " + s.getSource.op + " sink " + sink.port)
        smeta.addSink(sink.port)
        configurePorts(s, sink)
      })
      if (s.getLocality != null) {
        println("setting locality of the stream " + s.getLocality)
        smeta.setLocality(s.getLocality)
      }
    })

    for(o <- opScaleMap) {
      dag.getMeta(o._1).getAttributes.put(OperatorContext.PARTITIONER, new StatelessPartitioner(o._2))
    }

  }

  override def setscale[A](source : Source[A], num : Int): Unit = {
    val oldScale = opScaleMap.getOrElse(source.op, 1)
    opScaleMap.put(source.op, Math.max(oldScale, num))
  }

  def configurePorts[T](s : Stream[T], sink : Sink[T]) = {
  }
}

object DTContext {
  def newInstance : DTContext = {
    val dag = new LogicalPlan()
    val conf = new Configuration()
    new DTContext(dag, conf)
  }
}
