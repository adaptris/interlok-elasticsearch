package com.adaptris.core.elastic;

import org.elasticsearch.common.xcontent.XContentBuilder;

public class DocumentWrapper {

  private final String uniqueId;
  private final XContentBuilder content;
  
  public DocumentWrapper(String uid, XContentBuilder content) {
    this.uniqueId = uid;
    this.content = content;
  }
  
  public XContentBuilder content() {
    return content;
  }

  public String uniqueId() {
    return uniqueId;
  }

}
