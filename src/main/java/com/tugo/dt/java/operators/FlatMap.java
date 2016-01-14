package com.tugo.dt.java.operators;

import java.io.Serializable;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

@DefaultSerializer(JavaSerializer.class)
public class FlatMap<A,B> extends BaseSinglePortOperator<A,B> implements Serializable
{
  public interface FlatMapFunc<A,B> extends Serializable {
    Iterable<B> apply(A tuple);
  }

  private FlatMapFunc<A,B> func;

  public FlatMap(FlatMapFunc func) {
    this.func = func;
  }

  @Override
  protected void processTuple(A tuple)
  {
    Iterable<B> items = func.apply(tuple);
    for(B item : items) {
      output.emit(item);
    }
  }
}
