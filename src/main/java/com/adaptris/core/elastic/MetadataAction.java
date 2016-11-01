package com.adaptris.core.elastic;

import javax.validation.constraints.NotNull;

import com.adaptris.core.AdaptrisMessage;

public class MetadataAction implements ActionExtractor {

  @NotNull
  private String metadataKey;

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) {
    return msg.getMetadataValue(metadataKey());
  }

  public String getMetadataKey() {
    return metadataKey;
  }

  public void setMetadataKey(String metadataKey) {
    this.metadataKey = metadataKey;
  }
  
  private String metadataKey() {
    return getMetadataKey() != null ? getMetadataKey() : "action";
  }

}
