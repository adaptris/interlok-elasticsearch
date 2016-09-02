package com.adaptris.core.elastic;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.mail.URLName;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.core.CoreException;
import com.adaptris.core.NoOpConnection;
import com.adaptris.core.util.Args;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairBag;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * 
 * @author lchan
 * @config elasticsearch-connection
 */
@XStreamAlias("elasticsearch-connection")
public class ElasticSearchConnection extends NoOpConnection {

  @XStreamImplicit(itemFieldName = "transport-url")
  @Size(min = 1)
  @Valid
  private List<String> transportUrls;

  @NotNull
  @AutoPopulated
  @Valid
  private KeyValuePairSet settings = null;

  @NotBlank
  private String index = null;

  public ElasticSearchConnection() {
    setTransportUrls(new ArrayList<String>());
    setSettings(new KeyValuePairSet());
  }

  public ElasticSearchConnection(String index) {
    this();
    setIndex(index);
  }

  protected TransportClient createClient() throws CoreException {
    Settings s = Settings.settingsBuilder().put(asMap(getSettings())).build();
    TransportClient transportClient = TransportClient.builder().settings(s).build();
    for (String url : getTransportUrls()) {
      transportClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(getHost(url), getPort(url))));
    }
    return transportClient;
  }

  public KeyValuePairSet getSettings() {
    return settings;
  }

  public void setSettings(KeyValuePairSet kvps) {
    this.settings = Args.notNull(kvps, "Settings");
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

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = Args.notBlank(index, "index");
  }

  protected void closeQuietly(TransportClient c) {
    closeQuietly(c);
  }

  private static void closeQuietly(Releasable c) {
    try {
      if (c != null) {
        c.close();
      }
    }
    catch (Exception e) {
      ;
    }
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
    }
    else {
      result = hostUrl.substring(0, hostUrl.lastIndexOf(":"));
    }
    return result;
  }

  private static Integer getPort(String hostUrl) {
    Integer result = 0;
    if (hostUrl.contains("://")) {
      result = new URLName(hostUrl).getPort();
    }
    else {
      String s = hostUrl.substring(hostUrl.lastIndexOf(":") + 1);
      s.replaceAll("/", "");
      result = Integer.parseInt(s);
    }
    return result;
  }
}
