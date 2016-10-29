package com.adaptris.core.elastic;

import org.elasticsearch.common.xcontent.XContentBuilder;

class DocumentWrapper {

  /**
   * What to do with this document. These are references by ordinal from the 
   * "Delta_Status" column so the ordering in this enum is important!
   */
  public enum Action { DELETE, UPDATE, INDEX }
  
  private final Action action;
  private final String uniqueId;
  private final XContentBuilder content;
  
  public DocumentWrapper(Action action, String uid, XContentBuilder content) {
    this.action = action;
    this.uniqueId = uid;
    this.content = content;
  }
  
  Action action() {
    return action;
  }
  
  XContentBuilder content() {
    return content;
  }

  String uniqueId() {
    return uniqueId;
  }

}
