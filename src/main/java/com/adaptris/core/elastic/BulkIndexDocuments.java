package com.adaptris.core.elastic;

import javax.validation.constraints.Min;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.services.splitter.CloseableIterable;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Add a document(s) to ElasticSearch.
 * 
 * <p>
 * {@link ProduceDestination#getDestination(AdaptrisMessage)} should return the type of document that we are submitting to into
 * ElasticSearch; the {@code index} is taken from the underlying {@link ElasticSearchConnection}.
 * </p>
 * 
 * @author lchan
 * @config elasticsearch-bulk-index-document
 *
 */
@XStreamAlias("elasticsearch-bulk-index-document")
public class BulkIndexDocuments extends IndexDocuments {

  private static final int DEFAULT_BATCH_WINDOW = 10000;

  @Min(0)
  private Integer batchWindow;

  public BulkIndexDocuments() {
    super();
  }


  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, ProduceDestination destination, long timeout) throws ProduceException {
    try {
      final String type = destination.getDestination(msg);
      final String index = retrieveConnection(ElasticSearchConnection.class).getIndex();
      BulkRequestBuilder bulk = transportClient.prepareBulk();
      try (CloseableIterable<DocumentWrapper> docs = ensureCloseable(getDocumentBuilder().build(msg))) {
        int count = 0;
        for (DocumentWrapper doc : docs) {
          count++;
          bulk.add(transportClient.prepareIndex(index, type, doc.uniqueId()).setSource(doc.content()));
          if (count >= batchWindow()) {
            doSend(bulk);
            bulk = transportClient.prepareBulk();
          }
        }
      }
      if (bulk.numberOfActions() > 0) {
        doSend(bulk);
      }
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }

  private void doSend(BulkRequestBuilder bulk) throws Exception {
    BulkResponse response = bulk.get();
    if (response.hasFailures()) {
      throw new ProduceException(response.buildFailureMessage());
    }
    return;
  }


  /**
   * @return the batchCount
   */
  public Integer getBatchWindow() {
    return batchWindow;
  }

  /**
   * @param b the batchCount to set
   */
  public void setBatchWindow(Integer b) {
    this.batchWindow = b;
  }

  int batchWindow() {
    return getBatchWindow() != null ? getBatchWindow().intValue() : DEFAULT_BATCH_WINDOW;
  }

}
