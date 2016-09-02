package com.adaptris.core.elastic;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.services.splitter.CloseableIterable;

public class CsvDocumentBuilderTest {

  private static final String CSV_INPUT = "Name,Order Date,Date Attending,Total Paid" + System.lineSeparator()
      + "Record1,\"Sep 15, 2012\",\"Oct 22, 2012 at 6:00 PM\",0" + System.lineSeparator()
      + "Record2,\"Sep 16, 2012\",\"Oct 22, 2012 at 6:00 PM\",0" + System.lineSeparator()
      + "Record3,\"Sep 17, 2012\",\"Oct 22, 2012 at 6:00 PM\",0" + System.lineSeparator();

  @Rule
  public TestName testName = new TestName();

  @Test
  public void testBuild() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    msg.addMetadata(testName.getMethodName(), testName.getMethodName());
    CSVDocumentBuilder documentBuilder = new CSVDocumentBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = ElasticSearchProducer.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        assertEquals("Record" + count, doc.uniqueId());
        System.out.println(doc.content().string());
      }
    }
    assertEquals(3, count);
  }

}
