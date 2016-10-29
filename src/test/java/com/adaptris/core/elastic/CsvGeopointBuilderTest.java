package com.adaptris.core.elastic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedHashMap;

import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.elastic.DocumentWrapper.Action;
import com.adaptris.core.services.splitter.CloseableIterable;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;

public class CsvGeopointBuilderTest extends CsvBuilderCase {

  private static final String JSON_LOCATION = "$.location";

  public static final String CSV_WITH_LATLONG =
      "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,latitude,longitude,recordid,id"
          + System.lineSeparator()
          + "UID-1,24-D Amine,Passion Fruit,Herbicides,19,20080506,,2.8,Litres per Hectare,,0,53.37969768091292,-0.18346963126415416,210,209"
          + System.lineSeparator()
          + "UID-2,26N35S,Rape Winter,Fungicides,12,20150314,,200,Kilograms per Hectare,,0,52.71896363632868,-1.2391368098336788,233,217"
          + System.lineSeparator();
  
  public static final String CSV_WITH_LATLONG_AND_DELTA =
      "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,latitude,longitude,recordid,id,delta_status"
          + System.lineSeparator()
          + "UID-1,24-D Amine,Passion Fruit,Herbicides,19,20080506,,2.8,Litres per Hectare,,0,53.37969768091292,-0.18346963126415416,210,209,0"
          + System.lineSeparator()
          + "UID-2,26N35S,Rape Winter,Fungicides,12,20150314,,200,Kilograms per Hectare,,0,52.71896363632868,-1.2391368098336788,233,217,1"
          + System.lineSeparator()
          + "UID-3,26N35S,Rape Winter,Fungicides,12,20150314,,200,Kilograms per Hectare,,0,52.71896363632868,-1.2391368098336788,233,217,2"
          + System.lineSeparator();

  public static final String CSV_WITHOUT_LATLONG =
      "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,latitude,longitude,recordid,id"
          + System.lineSeparator() + "UID-1,*A Simazine,,Insecticides,48,20051122,,1.5,Litres per Hectare,,0,,,5,1"
          + System.lineSeparator() + "UID-2,*Axial,,Herbicides,15,20100408,,0.25,Litres per Hectare,,0,,,6,6"
          + System.lineSeparator() + "UID-3,*Betanal Maxxim,,Herbicides,18,20130501,,0.07,Litres per Hectare,,0,,,21,21"
          + System.lineSeparator();

  @Override
  protected CSVWithGeoPointBuilder createBuilder() {
    return new CSVWithGeoPointBuilder();
  }


  @Test
  public void testBuild_WithLatLong() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_WITH_LATLONG);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = ElasticSearchProducer.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(doc.content().string());
        assertEquals(Action.INDEX, doc.action());
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID));
        LinkedHashMap map = context.read(JSON_LOCATION);
        assertTrue(map.containsKey("lat"));
        assertFalse("0".equals(map.get("lat").toString()));
        assertTrue(map.containsKey("lon"));
        assertFalse("0".equals(map.get("lon").toString()));
      }
    }
    assertEquals(2, count);
  }
  
  @Test
  public void testBuild_WithCustomLatLong() throws Exception {
    String payload = CSV_WITH_LATLONG.replace("latitude", "my_lat").replace("longitude", "my_lon");
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(payload);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder();
    documentBuilder.setLatitudeFieldNames("My_Lat");
    documentBuilder.setLongitudeFieldNames("My_Lon");
    documentBuilder.setLocationFieldName("My_Location");
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = ElasticSearchProducer.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(doc.content().string());
        assertEquals(Action.INDEX, doc.action());
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID));
        LinkedHashMap map = context.read("$.My_Location");
        assertTrue(map.containsKey("lat"));
        assertFalse("0".equals(map.get("lat").toString()));
        assertTrue(map.containsKey("lon"));
        assertFalse("0".equals(map.get("lon").toString()));
      }
    }
    assertEquals(2, count);
  }
  
  @Test
  public void testBuild_WithLatLongAndDelta() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_WITH_LATLONG_AND_DELTA);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = ElasticSearchProducer.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(doc.content().string());
        assertEquals(Action.values()[count-1], doc.action());
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID));
        LinkedHashMap map = context.read(JSON_LOCATION);
        assertTrue(map.containsKey("lat"));
        assertFalse("0".equals(map.get("lat").toString()));
        assertTrue(map.containsKey("lon"));
        assertFalse("0".equals(map.get("lon").toString()));
      }
    }
    assertEquals(3, count);
  }

  @Test
  public void testBuild_WithoutLatLong() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_WITHOUT_LATLONG);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = ElasticSearchProducer.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(doc.content().string());
        assertEquals(Action.INDEX, doc.action());
        assertEquals("UID-" + count, context.read("$.productuniqueid"));
        try {
          LinkedHashMap map = context.read(JSON_LOCATION);
          fail();
        } catch (PathNotFoundException expected) {
          ;
        }
      }
    }
    assertEquals(3, count);
  }


}
