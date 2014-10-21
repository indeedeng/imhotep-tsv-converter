package com.indeed.imhotep.index.builder.util;

/**
 * @author jchien
 */
public interface EncodedValueHandler {
    // this method is called on each extracted value to modify it (e.g. scale the value down) before encoding
    int transform(int value);
}
