package com.adaptris.core.elastic;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.adaptris.core.transform.csv.BasicFormatBuilder;
import com.adaptris.core.transform.csv.FormatBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Builds a document for elastic search.
 * 
 * <p>
 * The document that is created contains the following characteristics
 * <ul>
 * <li>The first record of the CSV is assumed to be a header row, and is used as the fieldName for each entry</li>
 * <li>The "unique-id" for the document is derived from the specified column, duplicates may have unexpected results depending on
 * your configuration; generally will be an updated version number</li>
 * <li>Any fields which matching "latitude"/"longitude" are aggregated and created as a {@code location} field.
 * </ul>
 * </p>
 * 
 * @author lchan
 * @config elasticsearch-csv-geopoint-document-builder
 *
 */
@XStreamAlias("elasticsearch-csv-geopoint-document-builder")
public class CSVWithGeoPointBuilder extends CSVDocumentBuilderImpl {

  private static final List<String> LATITUDE = Arrays.asList(new String[]
  {
      "latitude", "lat"
  });
  private static final List<String> LONGITUDE = Arrays.asList(new String[]
  {
      "longitude", "lon",
  });

  private static final List<String> LAT_OR_LONG = new ArrayList<>(CollectionUtils.union(LATITUDE, LONGITUDE));

  public CSVWithGeoPointBuilder() {
    this(new BasicFormatBuilder());
  }

  public CSVWithGeoPointBuilder(FormatBuilder f) {
    setFormat(f);
  }

  @Override
  protected CSVDocumentWrapper buildWrapper(CSVParser parser) {
    return new MyWrapper(parser);
  }

  private class MyWrapper extends CSVDocumentWrapper {
    private List<String> headers = new ArrayList<>();
    private LatLongHandler latLong;

    public MyWrapper(CSVParser p) {
      super(p);
      headers = buildHeaders(csvIterator.next());
      latLong = new LatLongHandler(headers);
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
          if (!LAT_OR_LONG.contains(fieldName.toLowerCase())) {
            String data = record.get(i);
            builder.field(fieldName, new Text(data));
          }
        }
        latLong.addLatLong(builder, record);
        builder.endObject();
        result = new DocumentWrapper(uniqueId, builder);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      return result;
    }
  }

  private class LatLongHandler {

    private int lat = -1;
    private int lon = -1;

    LatLongHandler(List<String> headers) {
      for (int i = 0; i < headers.size(); i++) {
        if (LATITUDE.contains(headers.get(i).toLowerCase())) {
          lat = i;
        }
        if (LONGITUDE.contains(headers.get(i).toLowerCase())) {
          lon = i;
        }
      }
    }

    void addLatLong(XContentBuilder builder, CSVRecord record) throws IOException {
      if (lat == -1 || lon == -1) {
        return;
      }
      String latitude = record.get(lat);
      String longitude = record.get(lon);
      try {
        builder.latlon("location", Double.valueOf(latitude).doubleValue(), Double.valueOf(longitude).doubleValue());
      }
      catch (NumberFormatException e) {
        // Ignore it, no chance of having a location, because the values aren't real latlongs.
      }
    }
  }
}
