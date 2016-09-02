package com.adaptris.core.elastic;

import org.elasticsearch.common.xcontent.XContentBuilder;

class DocumentWrapper {

  private String uniqueId;
  private XContentBuilder content;

  public DocumentWrapper(String uid, XContentBuilder content) {
    this.uniqueId = uid;
    this.content = content;
  }
  
  XContentBuilder content() {
    return content;
  }

  String uniqueId() {
    return uniqueId;
  }

}
