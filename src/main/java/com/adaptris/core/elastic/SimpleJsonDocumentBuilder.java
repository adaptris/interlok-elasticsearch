package com.adaptris.core.elastic;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Builds a simple document for elastic search.
 * 
 * <p>
 * The simple document that is created contains the following characteristics
 * <ul>
 * <li>{@code content} contains the message payload (as a String)</li>
 * <li>{@code metadata} all the metadata (removing illegal values, such as metadata keys with '.' in them</li>
 * <li>{@code date} contains the current date/time</li>
 * </ul>
 * </p>
 * 
 * @author lchan
 * @config elasticsearch-simple-document-builder
 *
 */
@XStreamAlias("elasticsearch-simple-document-builder")
public class SimpleJsonDocumentBuilder implements ElasticDocumentBuilder {

  private transient Logger log = LoggerFactory.getLogger(this.getClass());

  @Override
  public Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException {
    List<DocumentWrapper> result = new ArrayList<>();
    try {
      XContentBuilder builder = jsonBuilder();
      builder.startObject();
      builder.field("content", new Text(msg.getContent()));
      builder.field("metadata", filterIllegal(msg.getMessageHeaders()));
      builder.field("date", new Date());
      builder.endObject();
      result.add(new DocumentWrapper(msg.getUniqueId(), builder));
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return result;
  }

  private static Map<String, String> filterIllegal(Map<String, String> map) {
    Map<String, String> result = new HashMap<>();
    map.entrySet().stream().filter(e -> !e.getKey().contains(".")).forEach(e -> {
      result.put(e.getKey(), e.getValue());
    });
    return result;
  }

}
