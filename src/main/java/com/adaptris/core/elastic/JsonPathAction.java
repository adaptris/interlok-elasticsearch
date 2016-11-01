package com.adaptris.core.elastic;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("elasticsearch-jsonpath-action")
public class JsonPathAction implements ActionExtractor {

  @NotNull
  private String jsonPath;
  
  public JsonPathAction() {
    setJsonPath("$.action");
  }

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) throws ServiceException {
    try {
      String content = document.content().string();
      ReadContext context = JsonPath.parse(content);
      return context.read(getJsonPath());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  public String getJsonPath() {
    return jsonPath;
  }

  public void setJsonPath(String jsonPath) {
    this.jsonPath = jsonPath;
  }

}
