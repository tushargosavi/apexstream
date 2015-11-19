package com.tugo.dt.scala.dag

import com.datatorrent.api.Operator.{OutputPort, InputPort}
import com.datatorrent.api.{DAG, Operator}
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.FileLineInputOperator
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DTContext(val dag : DAG, conf : Configuration) {

  val strm = new ListBuffer[(StreamInfo, StreamInfo)]
  val smap = new mutable.HashMap[Stream[_], StreamInfo]()

  def addStream(s: Stream[_]) = {
    if (!smap.contains(s))
      smap.+=((s, new StreamInfo(s)))
  }

  def addStream(prev : Stream[_], cur : Stream[_]): Unit = {
    addStream(cur)
    if (prev != null && cur != null)
      strm.append((smap.get(prev).orNull, smap.get(cur).orNull))
  }

  def source[A](op : Operator) : Stream[A] = {
    val stream = new com.tugo.dt.scala.dag.Stream[A](this, null, op)
    addStream(stream)
    stream
  }

  def fileInput(dir : String) : Stream[String] = {
    val op = new FileLineInputOperator()
    op.setDirectory(dir)
    source[String](op)
  }

  class StreamInfo(val stream : Stream[_]) {
    val ports = PortMapper.mapPorts(stream.op)
    val name = stream.op.getClass.getName
    val op = stream.op

    def input : InputPort[_] = ports._1.values.toList.head
    def output : OutputPort[_] = ports._2.values.toList.head
  }

  var id : Int = 0
  def nextId = { id = id + 1; id }

  def build(): Unit = {
    val sinfos = smap.values
    sinfos.foreach(i => {
      println("adding operator " + i.name)
      dag.addOperator(i.name + nextId, i.op)
    })
    strm.foreach(con => {
      println("adding stream " + con._1.name + " to " + con._2.name)
      val smeta = dag.addStream("s" + nextId)
      smeta.setSource(con._1.output)
      smeta.addSink(con._2.input)
    })
  }

  def submit(): Unit = {

  }
}
