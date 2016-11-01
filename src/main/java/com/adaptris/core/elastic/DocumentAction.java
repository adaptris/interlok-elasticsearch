package com.adaptris.core.elastic;

/**
 * What to do with this document. These are references by ordinal from the 
 * "Delta_Status" column so the ordering in this enum is important!
 */
public enum DocumentAction { DELETE, UPDATE, INDEX }