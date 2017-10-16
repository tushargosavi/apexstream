package com.tugo.dt.scala

import com.datatorrent.api.LocalMode
import org.apache.hadoop.conf.Configuration

class AppTest {
  def test = {
    val lma: LocalMode = LocalMode.newInstance
    val conf: Configuration = new Configuration(false)
    conf.addResource(this.getClass.getResourceAsStream("/META-INF/properties.xml"))
    lma.prepareDAG(new TestApp, conf)
    val lc: LocalMode.Controller = lma.getController
    lc.run(10000)
  }
}
