package com.tugo.dt.scala

import com.datatorrent.api.annotation.ApplicationAnnotation
import com.datatorrent.api.{DAG, StreamingApplication}
import com.tugo.dt.scala.dag.DTContext
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ListBuffer


@ApplicationAnnotation(name="TestApp")
class TestApp extends StreamingApplication {
  override def populateDAG(dag: DAG, conf: Configuration): Unit = {
    val ctx : DTContext = new DTContext(dag, conf)
    val start = ctx.fileInput("/user/tushar/data")
    start.flatMap(_.split(" ")).filter(_.length > 0).uniqueCount.flatMap(map => {
      val lst = new ListBuffer[String]
      for (a <- map.entrySet().toArray()) {
        lst.append(a.toString)
      }
      lst
    }).print()
    ctx.build
  }

}
