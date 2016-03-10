package com.tugo.dt.scala.streams

object DagTests {

  def testDAG = {
    import com.tugo.dt.scala.streams.DTContext
    import com.datatorrent.stram.plan.logical.LogicalPlan
    import org.apache.hadoop.conf.Configuration

    val ctx = new DTContext(new LogicalPlan(), new Configuration())
    val input = ctx.fileInput("/tmp/tushar").flatMap(_.split(" ")).filter(_.length > 0)
    ctx.build

  }

  testDAG
}
