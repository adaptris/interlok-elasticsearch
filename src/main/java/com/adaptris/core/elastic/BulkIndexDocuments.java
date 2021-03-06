package com.adaptris.core.elastic;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.CloseableIterable;
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
  
  @AdvancedConfig
  @Valid
  private ActionExtractor action;

  public BulkIndexDocuments() {
    super();
    ConfiguredAction ca = new ConfiguredAction();
    ca.setAction(DocumentAction.INDEX);
    setAction(ca);
  }

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, ProduceDestination destination, long timeout) throws ProduceException {
    try {
      final String type = destination.getDestination(msg);
      final String index = retrieveConnection(ElasticSearchConnection.class).getIndex();
      BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
      try (CloseableIterable<DocumentWrapper> docs = ensureCloseable(getDocumentBuilder().build(msg))) {
        int count = 0;
        for (DocumentWrapper doc : docs) {
          count++;
          DocumentAction action = DocumentAction.valueOf(getAction().extract(msg, doc));
          switch(action) {
          case INDEX:
            bulkRequest.add(transportClient.prepareIndex(index, type, doc.uniqueId()).setSource(doc.content()));
            break;
          case UPDATE:
            bulkRequest.add(transportClient.prepareUpdate(index, type, doc.uniqueId()).setDoc(doc.content()));
            break;
          case DELETE:
            bulkRequest.add(transportClient.prepareDelete(index, type, doc.uniqueId()));
            break;
          default:
            throw new ProduceException("Unrecognized action: " + action);
          }
          if (count >= batchWindow()) {
            doSend(bulkRequest);
            count = 0;
            bulkRequest = transportClient.prepareBulk();
          }
        }
      }
      if (bulkRequest.numberOfActions() > 0) {
        doSend(bulkRequest);
      }
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }

  private void doSend(BulkRequestBuilder bulkRequest) throws Exception {
    int count = bulkRequest.numberOfActions();
    BulkResponse response = bulkRequest.get();
    if (response.hasFailures()) {
      throw new ProduceException(response.buildFailureMessage());
    }
    log.trace("Producing batch of {} actions took {}", count, response.getTook().toString());
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


  public ActionExtractor getAction() {
    return action;
  }


  public void setAction(ActionExtractor action) {
    this.action = action;
  }

}
