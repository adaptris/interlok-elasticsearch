package com.adaptris.core.elastic;

import static org.junit.Assert.*;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairList;

public class ActionExtractorCase {

  @Test
  public void testConstantAction() {
    for(DocumentAction val: DocumentAction.values()) {
      ConfiguredAction action = new ConfiguredAction();
      action.setAction(val);
      assertEquals(val.name(), action.extract(null, null));
    }
  }
  
  @Test
  public void testMetadataAction() {
    final String KEY = "my_action";
    MetadataAction action = new MetadataAction();
    action.setMetadataKey(KEY);
    for(DocumentAction val: DocumentAction.values()) {
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      msg.addMetadata(KEY, val.name());
      assertEquals(val.name(), action.extract(msg, null));
    }
  }
  
  @Test
  public void testJsonPathAction() throws IOException, ServiceException {
    final String FIELD_NAME = "myaction";
    final String JSON_PATH = "$." + FIELD_NAME;
    JsonPathAction action = new JsonPathAction();
    action.setJsonPath(JSON_PATH);
    for(DocumentAction val: DocumentAction.values()) {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      builder.field(FIELD_NAME, val.name());
      builder.endObject();
      assertEquals(val.name(), action.extract(null, new DocumentWrapper("uid", builder)));
    }
  }
  
  @Test
  public void testMappedAction() throws ServiceException {
    final String KEY = "myaction";
    MetadataAction ma = new MetadataAction();
    ma.setMetadataKey(KEY);
    MappedAction action = new MappedAction();
    action.setAction(ma);
    KeyValuePairList mappings = new KeyValuePairList();
    for(DocumentAction val: DocumentAction.values()) {
      mappings.add(new KeyValuePair(val.name().substring(0, 1), val.name()));
    }
    action.setMappings(mappings);
    
    for(DocumentAction val: DocumentAction.values()) {
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      msg.addMetadata(KEY, val.name().substring(0, 1));
      
      assertEquals(val, DocumentAction.valueOf(action.extract(msg, null)));
    }
  }

}
