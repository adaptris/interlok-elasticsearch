package com.adaptris.core.elastic;

import java.util.EnumSet;

import org.junit.Rule;
import org.junit.rules.TestName;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

public abstract class BuilderCase {

  @Rule
  public TestName testName = new TestName();

  private Configuration jsonConfig = new Configuration.ConfigurationBuilder().jsonProvider(new JsonSmartJsonProvider())
      .mappingProvider(new JacksonMappingProvider()).options(EnumSet.noneOf(Option.class)).build();

  protected ReadContext parse(String content) {
    return JsonPath.parse(content, jsonConfig);
  }



}
