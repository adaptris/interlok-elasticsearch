package com.adaptris.core.elastic;

import com.adaptris.core.ServiceCase;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

public class SimpleElasticSearchIndexTest extends ServiceCase {

  private static final String EXAMPLE_COMMENT_HEADER = "\n<!--" + "\n-->\n";

  public SimpleElasticSearchIndexTest(String name) {
    super(name);
  }

  public void testNoOp() throws Exception {

  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    KeyValuePairSet settings = new KeyValuePairSet();
    settings.add(new KeyValuePair("cluster.name", "my-cluster"));
    SimpleElasticSearchIndex search = new SimpleElasticSearchIndex("myIndex", "myType");
    search.setSettings(settings);
    search.addTransportUrl("localhost:9300");
    return search;
  }

  @Override
  protected String getExampleCommentHeader(Object o) {
    return super.getExampleCommentHeader(o) + EXAMPLE_COMMENT_HEADER;
  }

}
