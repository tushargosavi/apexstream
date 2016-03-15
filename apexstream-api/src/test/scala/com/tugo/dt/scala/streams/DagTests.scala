package com.tugo.dt.scala.streams

object DagTests {

  def testDAG = {
    import com.tugo.dt.scala.streams.DTContext
    import com.datatorrent.stram.plan.logical.LogicalPlan
    import org.apache.hadoop.conf.Configuration
    import com.datatorrent.lib.algo.UniqueCounter
    import scala.collection.JavaConverters._

    val ctx = new DTContext(new LogicalPlan(), new Configuration())
    val input = ctx.fileInput("/tmp/tushar").flatMap(_.split(" ")).filter(_.length > 0).addOperator[java.util.Map[String, Int]](new UniqueCounter()).flatMap(_.asScala.map(_.toString())).print()
    ctx.build

  }

  testDAG
}
