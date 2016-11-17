package com.adaptris.core.elastic;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("elasticsearch-lowercase-field-name-mapper")
public class ToLowerCaseFieldNameMapper implements FieldNameMapper {

  @Override
  public String map(String name) {
    return name.toLowerCase();
  }

}
