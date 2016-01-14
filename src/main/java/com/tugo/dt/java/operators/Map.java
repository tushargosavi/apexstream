package com.tugo.dt.java.operators;

import java.io.Serializable;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

@DefaultSerializer(JavaSerializer.class)
public class Map<A,B> extends BaseSinglePortOperator<A,B> implements Serializable
{
  public interface MapFunc<A,B> extends Serializable {
    B apply(A tuple);
  }

  MapFunc<A,B> func;

  public Map(MapFunc<A,B> func) {
    this.func = func;
  }

  @Override
  protected void processTuple(A tuple)
  {
    output.emit(func.apply(tuple));
  }
}
