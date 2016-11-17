package com.adaptris.core.elastic;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FieldNameMapperTest {
  final String INPUT = "abcABC";

  @Test
  public void testNoOpMapper() {
    FieldNameMapper mapper = new NoOpFieldNameMapper();
    assertEquals(INPUT, mapper.map(INPUT));
  }
  
  @Test
  public void testToUpperCaseMapper() {
    FieldNameMapper mapper = new ToUpperCaseFieldNameMapper();
    assertEquals(INPUT.toUpperCase(), mapper.map(INPUT));
  }

  @Test
  public void testToLowerCaseMapper() {
    FieldNameMapper mapper = new ToLowerCaseFieldNameMapper();
    assertEquals(INPUT.toLowerCase(), mapper.map(INPUT));
  }

}
