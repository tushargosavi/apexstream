package com.tugo.dt.application;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import com.tugo.dt.java.operators.FlatMap;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

@ApplicationAnnotation(name = "JavaWordCount")
public class JavaWordCount implements StreamingApplication, Serializable
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {

    AbstractFileInputOperator.FileLineInputOperator in = dag.addOperator("Input", new AbstractFileInputOperator.FileLineInputOperator());
    in.setDirectory("/user/tushar/data");

    FlatMap<String, String> splitter = dag.addOperator("Splitter", new FlatMap<String, String>(new FlatMap.FlatMapFunc<String, String>()
    {
      @Override
      public Iterable apply(String tuple)
      {
        return Arrays.asList(tuple.split(" "));
      }
    }));

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("lines", in.output, splitter.input);
    dag.addStream("words", splitter.output, console.input);
  }
}
