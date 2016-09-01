package com.adaptris.core.elastic;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.mail.URLName;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairBag;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

@XStreamAlias("simple-elastic-search-insert")
public class SimpleElasticSearchIndex extends ServiceImp {

  // Relies on elasticsearch.yml being around for now.
  private transient TransportClient transportClient = null;

  @XStreamImplicit(itemFieldName = "transport-url")
  private List<String> transportUrls;
  private KeyValuePairSet settings = null;
  @NotBlank
  private String index = null;
  @NotBlank
  private String type = null;


  public SimpleElasticSearchIndex() {
    setTransportUrls(new ArrayList<String>());
    setSettings(new KeyValuePairSet());
  }

  public SimpleElasticSearchIndex(String index, String type) {
    this();
    setIndex(index);
    setType(type);
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      XContentBuilder builder = jsonBuilder();
      builder.startObject();
      builder.field("content", new StringText(msg.getContent()));
      builder.field("metadata", filterIllegal(msg.getMessageHeaders()));
//      metadata.entrySet().stream().forEach(item -> {
//        try {
//          builder.startObject();
//          builder.field("key", item.getKey());
//          builder.field("value", item.getValue());
//          builder.endObject();
//        } catch (Exception e) {
//        }
//      });
//      builder.endArray();
      builder.field("date", new Date());
      builder.endObject();
      // TODO add Timeout config.
      IndexResponse response = transportClient.prepareIndex(getIndex(), getType(), msg.getUniqueId()).setSource(builder).get();
      log.trace("Added document {} version {} to {}", response.getId(), response.getVersion(), getIndex());
    } catch (Exception e) {
      ExceptionHelper.rethrowServiceException(e);
    }
  }

  @Override
  public void prepare() throws CoreException {

  }


  @Override
  protected void closeService() {
    closeQuietly(transportClient);
  }

  @Override
  protected void initService() throws CoreException {
    Settings s = Settings.settingsBuilder().put(asMap(getSettings())).build();
    transportClient = TransportClient.builder().settings(s).build();
    for (String url : getTransportUrls()) {
      transportClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(getHost(url), getPort(url))));
    }
  }

  public Node createNode(NodeBuilder nb) {
    return nb.node();
  }

  public KeyValuePairSet getSettings() {
    return settings;
  }

  public void setSettings(KeyValuePairSet kvps) {
    this.settings = Args.notNull(kvps, "Settings");
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = Args.notBlank(index, "index");
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = Args.notBlank(type, "type");
  }


  private static void closeQuietly(Releasable c) {
    try {
      if (c != null) {
        c.close();
      }
    } catch (Exception e) {
      ;
    }
  }

  public List<String> getTransportUrls() {
    return transportUrls;
  }

  public void setTransportUrls(List<String> transports) {
    this.transportUrls = Args.notNull(transports, "Transport URLS");
  }

  public void addTransportUrl(String url) {
    transportUrls.add(Args.notNull(url, "URL"));
  }

  private static Map<String, String> asMap(KeyValuePairBag kvps) {
    Map<String, String> result = new HashMap<>();
    for (KeyValuePair kvp : kvps.getKeyValuePairs()) {
      result.put(kvp.getKey(), kvp.getValue());
    }
    return result;
  }

  private static String getHost(String hostUrl) {
    String result = hostUrl;
    if (hostUrl.contains("://")) {
      result = new URLName(hostUrl).getHost();
    } else {
      result = hostUrl.substring(0, hostUrl.lastIndexOf(":"));
    }
    return result;
  }

  private static Integer getPort(String hostUrl) {
    Integer result = 0;
    if (hostUrl.contains("://")) {
      result = new URLName(hostUrl).getPort();
    } else {
      String s = hostUrl.substring(hostUrl.lastIndexOf(":") + 1);
      s.replaceAll("/", "");
      result = Integer.parseInt(s);
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
