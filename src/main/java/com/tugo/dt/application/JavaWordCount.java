package com.tugo.dt.application;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import com.tugo.dt.java.operators.Filter;
import com.tugo.dt.java.operators.FlatMap;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.algo.UniqueCounter;
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
    FlatMap<String, String> splitter = dag.addOperator("Splitter", new FlatMap<>((s) -> Arrays.asList(s.split(" "))));
    Filter<String> filter = dag.addOperator("Filter", new Filter<>((s) -> s.length() > 0));
    UniqueCounter<String> uc = dag.addOperator("UCount", new UniqueCounter<String>());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("s1", in.output, splitter.input);
    dag.addStream("s2", splitter.output, filter.input);
    dag.addStream("s3", filter.output, uc.data);
    dag.addStream("s3", uc.count, console.input);
  }
}
