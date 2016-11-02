package com.adaptris.core.elastic;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
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

  @AdvancedConfig
  @InputFieldDefault(value = "latitude,lat")
  private String latitudeFieldNames;
  
  @AdvancedConfig
  @InputFieldDefault(value = "longitude,lon")
  private String longitudeFieldNames;
  
  @AdvancedConfig
  @InputFieldDefault(value = "location")
  private String locationFieldName;

  @AdvancedConfig
  private String addTimestampField;
  
  public CSVWithGeoPointBuilder() {
    this(new BasicFormatBuilder());
  }

  public CSVWithGeoPointBuilder(FormatBuilder f) {
    setFormat(f);
  }  
  
  public String getLatitudeFieldNames() {
    return latitudeFieldNames;
  }

  public void setLatitudeFieldNames(String latitudeFieldNames) {
    this.latitudeFieldNames = latitudeFieldNames;
  }

  private String latitudeFieldNames() {
    return getLatitudeFieldNames() != null ? getLatitudeFieldNames() : "latitude,lat";
  }
  
  public String getLongitudeFieldNames() {
    return longitudeFieldNames;
  }

  public void setLongitudeFieldNames(String longitudeFieldNames) {
    this.longitudeFieldNames = longitudeFieldNames;
  }
  
  private String longitudeFieldNames() {
    return getLongitudeFieldNames() != null ? getLongitudeFieldNames() : "longitude,lon";
  }

  public String getLocationFieldName() {
    return locationFieldName;
  }

  public void setLocationFieldName(String locationFieldName) {
    this.locationFieldName = locationFieldName;
  }
  
  private String locationFieldName() {
    return getLocationFieldName() != null ? getLocationFieldName() : "location";
  }

  public String getAddTimestampField() {
    return addTimestampField;
  }

  public void setAddTimestampField(String addTimestampField) {
    this.addTimestampField = addTimestampField;
  }
  
  private String addTimestampField() {
    return getAddTimestampField() != null ? getAddTimestampField() : null;
  }
  
  @Override
  protected CSVDocumentWrapper buildWrapper(CSVParser parser) {
    Set<String> latitudeFieldNames = new HashSet<String>(Arrays.asList(latitudeFieldNames().toLowerCase().split(",")));
    Set<String> longitudeFieldNames = new HashSet<String>(Arrays.asList(longitudeFieldNames().toLowerCase().split(",")));
    return new MyWrapper(latitudeFieldNames, longitudeFieldNames, parser);
  }

  private class MyWrapper extends CSVDocumentWrapper {
    private List<String> headers = new ArrayList<>();
    private LatLongHandler latLong;

    public MyWrapper(Set<String> latitudeFieldNames, Set<String> longitudeFieldNames, CSVParser p) {
      super(p);
      headers = buildHeaders(csvIterator.next());
      latLong = new LatLongHandler(latitudeFieldNames, longitudeFieldNames, headers);
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
        
        if(addTimestampField() != null) {
          builder.field(addTimestampField(), new Date().getTime());
        }
        
        for (int i = 0; i < record.size(); i++) {
          String fieldName = headers.size() > 0 ? headers.get(i) : "field_" + i;
          String data = record.get(i);
          if (!latLong.isLatOrLong(fieldName)) {
            builder.field(getFieldNameMapper().map(fieldName), new Text(data));
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
    
    private final Set<String> latOrLongFieldNames;
    
    private int lat = -1;
    private int lon = -1;

    LatLongHandler(Set<String> latitudeFieldNames, Set<String> longitudeFieldNames, List<String> headers) {
      this.latOrLongFieldNames = new HashSet<String>(CollectionUtils.union(latitudeFieldNames, longitudeFieldNames));
      
      for (int i = 0; i < headers.size(); i++) {
        if (latitudeFieldNames.contains(headers.get(i).toLowerCase())) {
          lat = i;
        }
        if (longitudeFieldNames.contains(headers.get(i).toLowerCase())) {
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
        builder.latlon(
            getFieldNameMapper().map(locationFieldName()), 
            Double.valueOf(latitude).doubleValue(), Double.valueOf(longitude).doubleValue());
      }
      catch (NumberFormatException e) {
        // Ignore it, no chance of having a location, because the values aren't real latlongs.
      }
    }
    
    boolean isLatOrLong(String name) {
      return latOrLongFieldNames.contains(name.toLowerCase());
    }
  }
}
