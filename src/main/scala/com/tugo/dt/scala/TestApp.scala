package com.tugo.dt.scala

import com.datatorrent.api.annotation.ApplicationAnnotation
import com.datatorrent.api.{DAG, StreamingApplication}
import com.datatorrent.lib.algo.UniqueCounter
import com.tugo.dt.scala.dag.DTContext
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._

@ApplicationAnnotation(name="TestApp")
class TestApp extends StreamingApplication {

  override def populateDAG(dag: DAG, conf: Configuration): Unit = {

    val ctx : DTContext = new DTContext(dag, conf)

    ctx.fileInput("/user/tushar/data")
      .flatMap(_.split(" "))
      .filter(_.length > 0)
      .addOperator[java.util.Map[String, Int]](new UniqueCounter())
      .flatMap(_.asScala.map(_.toString()))
      .print()

    ctx.build
  }

}
