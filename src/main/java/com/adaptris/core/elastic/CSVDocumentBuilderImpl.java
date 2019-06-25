package com.adaptris.core.elastic;

import static org.apache.commons.lang.StringUtils.defaultIfBlank;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.transform.csv.BasicFormatBuilder;
import com.adaptris.core.transform.csv.FormatBuilder;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.CloseableIterable;
import com.adaptris.core.util.ExceptionHelper;

public abstract class CSVDocumentBuilderImpl implements ElasticDocumentBuilder {
  @NotNull
  @AutoPopulated
  @Valid
  private FormatBuilder format;
  @AdvancedConfig
  @Min(0)
  @InputFieldDefault(value = "0")
  private Integer uniqueIdField;
  @AdvancedConfig
  @NotNull
  @Valid
  private FieldNameMapper fieldNameMapper;

  protected transient Logger log = LoggerFactory.getLogger(this.getClass());

  public CSVDocumentBuilderImpl() {
    this(new BasicFormatBuilder());
    setFieldNameMapper(new NoOpFieldNameMapper());
  }

  public CSVDocumentBuilderImpl(FormatBuilder f) {
    setFormat(f);
  }

  public FormatBuilder getFormat() {
    return format;
  }

  public void setFormat(FormatBuilder csvFormat) {
    this.format = Args.notNull(csvFormat, "format");
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

  protected int uniqueIdField() {
    return getUniqueIdField() != null ? getUniqueIdField().intValue() : 0;
  }

  protected List<String> buildHeaders(CSVRecord hdrRec) {
    List<String> result = new ArrayList<>();
    for (String hdrValue : hdrRec) {
      result.add(safeName(hdrValue));
    }
    return result;
  }

  private String safeName(String input) {
    return defaultIfBlank(input, "").trim().replaceAll(" ", "_");
  }

  @Override
  public Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException {
    CSVDocumentWrapper result = null;
    try {
      CSVFormat format = getFormat().createFormat();
      CSVParser parser = format.parse(msg.getReader());
      result = buildWrapper(parser);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return result;
  }

  protected abstract CSVDocumentWrapper buildWrapper(CSVParser parser);
  
  public FieldNameMapper getFieldNameMapper() {
    return fieldNameMapper;
  }

  public void setFieldNameMapper(FieldNameMapper fieldNameMapper) {
    this.fieldNameMapper = Args.notNull(fieldNameMapper, "fieldNameMapper");
  }

  protected abstract class CSVDocumentWrapper implements CloseableIterable<DocumentWrapper>, Iterator {
    protected CSVParser parser;
    protected Iterator<CSVRecord> csvIterator;

    public CSVDocumentWrapper(CSVParser p) {
      parser = p;
      csvIterator = p.iterator();
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
    public void close() throws IOException {
      IOUtils.closeQuietly(parser);
    }

  }
}
