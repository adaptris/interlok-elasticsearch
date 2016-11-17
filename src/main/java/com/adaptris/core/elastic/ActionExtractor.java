package com.adaptris.core.elastic;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;

public interface ActionExtractor {
  public String extract(AdaptrisMessage msg, DocumentWrapper document) throws ServiceException;
}
