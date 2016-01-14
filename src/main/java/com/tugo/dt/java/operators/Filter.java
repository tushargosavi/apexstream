package com.tugo.dt.java.operators;

import java.io.Serializable;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

@DefaultSerializer(JavaSerializer.class)
public class Filter<A> extends BaseSinglePortOperator<A, A> implements Serializable
{
  public interface FilterFunc<A> extends Serializable {
    public boolean apply(A tuple);
  }

  FilterFunc<A> func;

  public Filter(FilterFunc<A> func) {
    this.func = func;
  }

  @Override
  protected void processTuple(A tuple)
  {
    if (func.apply(tuple))
      output.emit(tuple);
  }
}
