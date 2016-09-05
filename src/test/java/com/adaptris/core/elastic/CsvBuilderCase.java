package com.adaptris.core.elastic;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.services.splitter.CloseableIterable;
import com.jayway.jsonpath.ReadContext;

public abstract class CsvBuilderCase extends BuilderCase {

  public static final String JSON_PRODUCTUNIQUEID = "$.productuniqueid";
  public static final String CSV_INPUT =
      "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,latitude,longitude,recordid,id"
          + System.lineSeparator() + "UID-1,*A Simazine,,Insecticides,48,20051122,,1.5,Litres per Hectare,,0,,,5,1"
          + System.lineSeparator() + "UID-2,*Axial,,Herbicides,15,20100408,,0.25,Litres per Hectare,,0,,,6,6"
          + System.lineSeparator() + "UID-3,*Betanal Maxxim,,Herbicides,18,20130501,,0.07,Litres per Hectare,,0,,,21,21"
          + System.lineSeparator()
          + "UID-4,24-D Amine,Passion Fruit,Herbicides,19,20080506,,2.8,Litres per Hectare,,0,53.37969768091292,-0.18346963126415416,210,209"
          + System.lineSeparator()
          + "UID-5,26N35S,Rape Winter,Fungicides,12,20150314,,200,Kilograms per Hectare,,0,52.71896363632868,-1.2391368098336788,233,217"
          + System.lineSeparator();

  @Test
  public void testBuild() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    msg.addMetadata(testName.getMethodName(), testName.getMethodName());
    CSVDocumentBuilderImpl documentBuilder = createBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = ElasticSearchProducer.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(doc.content().string());
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID));
        assertEquals("UID-" + count, doc.uniqueId());
      }
    }
    assertEquals(5, count);
  }


  protected abstract CSVDocumentBuilderImpl createBuilder();

}
