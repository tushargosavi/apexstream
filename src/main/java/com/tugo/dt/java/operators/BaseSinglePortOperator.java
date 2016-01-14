package com.tugo.dt.java.operators;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

@DefaultSerializer(JavaSerializer.class)
public abstract class BaseSinglePortOperator<A,B> extends BaseOperator implements Serializable
{
  public transient DefaultOutputPort<B> output;
  public transient DefaultInputPort<A> input;

  private void init() {
    output = new DefaultOutputPort<>();

    input = new DefaultInputPort<A>() {
      @Override
      public void process(A tuple)
      {
        processTuple(tuple);
      }
    };
  }

  protected abstract void processTuple(A tuple);

  public BaseSinglePortOperator() {
    init();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
    init();
  }
}
