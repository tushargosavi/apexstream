/**
 * Put your copyright and license info here.
 */
package com.tugo.dt;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.tugo.dt.scala.TestApp;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Test
  public void testApplication1() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new TestApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  @Test
  public void test1() throws IOException {
    boolean b = BooleanUtils.toBoolean("true");
    System.out.println(b);

    b = BooleanUtils.toBoolean("false");
    System.out.println(b);

    b = BooleanUtils.toBoolean("true1");
    System.out.println(b);

    b = BooleanUtils.toBoolean((String)null);
    System.out.println(b);

    String[] arr = StringUtils.split("[tushar,gosavi]", ",");
    for (String a : arr) {
      System.out.println(a);
    }
  }
}


















