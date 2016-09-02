package com.adaptris.core.elastic;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.services.splitter.CloseableIterable;
import com.adaptris.core.transform.csv.BasicFormatBuilder;
import com.adaptris.core.transform.csv.FormatBuilder;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Builds a simple document for elastic search.
 * 
 * <p>
 * The document that is created contains the following characteristics
 * <ul>
 * <li>The first record of the CSV is assumed to be a header row, and is used as the fieldName for each entry</li>
 * <li>The "unique-id" for the document is derived from the specified column, duplicates may have unexpected results depending on
 * your configuration.</li>
 * <li>{@code date} contains the current date/time</li>
 * </ul>
 * </p>
 * 
 * @author lchan
 * @config elasticsearch-simple-document-builder
 *
 */
@XStreamAlias("elasticsearch-csv-document-builder")
public class CSVDocumentBuilder implements ElasticDocumentBuilder {

  @NotNull
  @AutoPopulated
  @Valid
  private FormatBuilder format;
  @AdvancedConfig
  @InputFieldDefault(value = "true")
  private Boolean useHeaderRecord;
  @AdvancedConfig
  @Min(0)
  @InputFieldDefault(value = "0")
  private Integer uniqueIdField;

  private transient Logger log = LoggerFactory.getLogger(this.getClass());

  public CSVDocumentBuilder() {
    this(new BasicFormatBuilder());
  }

  public CSVDocumentBuilder(FormatBuilder f) {
    setFormat(f);
  }

  @Override
  public Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException {
    CSVDocumentWrapper result = null;
    try {
      CSVFormat format = getFormat().createFormat();
      CSVParser parser = format.parse(msg.getReader());
      result = new CSVDocumentWrapper(parser);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return result;
  }

  public FormatBuilder getFormat() {
    return format;
  }

  public void setFormat(FormatBuilder csvFormat) {
    this.format = Args.notNull(csvFormat, "format");
  }

  public Boolean getUseHeaderRecord() {
    return useHeaderRecord;
  }

  /**
   * Whether or not the document contains a header row.
   * 
   * @param b the useHeaderRecord to set, defaults to true.
   */
  public void setUseHeaderRecord(Boolean b) {
    this.useHeaderRecord = b;
  }

  private boolean useHeaderRecord() {
    return getUseHeaderRecord() != null ? getUseHeaderRecord().booleanValue() : true;
  }

  public Integer getUniqueIdField() {
    return uniqueIdField;
  }

  /**
   * Specify which field is considered the unique-id
   * 
   * @param i the uniqueIdField to set, defaults to the first field (first field = '0').
   */
  public void setUniqueIdField(Integer i) {
    this.uniqueIdField = i;
  }

  private int uniqueIdField() {
    return getUniqueIdField() != null ? getUniqueIdField().intValue() : 0;
  }

  private List<String> buildHeaders(CSVRecord hdrRec) {
    List<String> result = new ArrayList<>();
    for (String hdrValue : hdrRec) {
      result.add(hdrValue);
    }
    return result;
  }

  private class CSVDocumentWrapper implements CloseableIterable<DocumentWrapper>, Iterator {
    private CSVParser parser;
    private Iterator<CSVRecord> csvIterator;
    private List<String> headers = new ArrayList<>();

    public CSVDocumentWrapper(CSVParser p) {
      parser = p;
      csvIterator = p.iterator();
      if (useHeaderRecord()) {
        headers = buildHeaders(csvIterator.next());
      }
    }

    @Override
    public Iterator<DocumentWrapper> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return csvIterator.hasNext();
    }

    @Override
    public DocumentWrapper next() {
      DocumentWrapper result = null;
      try {
        CSVRecord record = csvIterator.next();
        int idField = 0;
        if (uniqueIdField() <= record.size()) {
          idField = uniqueIdField();
        }
        else {
          throw new IllegalArgumentException("unique-id field > number of fields in record");
        }
        String uniqueId = record.get(idField);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        for (int i = 0; i < record.size(); i++) {
          String fieldName = headers.size() > 0 ? headers.get(i) : "field_" + i;
          String data = record.get(i);
          builder.field(fieldName, new Text(data));
        }
        builder.endObject();
        result = new DocumentWrapper(uniqueId, builder);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      return result;
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeQuietly(parser);
    }

  }

}
