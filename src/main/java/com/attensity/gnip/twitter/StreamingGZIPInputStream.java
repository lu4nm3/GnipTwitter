package com.attensity.gnip.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * @author lmedina
 */
public class StreamingGZIPInputStream extends GZIPInputStream {

    private final InputStream wrapped;
    public StreamingGZIPInputStream(InputStream is) throws IOException {
        super(is);
        wrapped = is;
    }

    /**
     * Overrides behavior of GZIPInputStream which assumes we have all the data available
     * which is not true for streaming. We instead rely on the underlying stream to tell us
     * how much data is available.
     *
     * Programs should not count on this method to return the actual number
     * of bytes that could be read without blocking.
     *
     * @return - whatever the wrapped InputStream returns
     * @exception  IOException  if an I/O error occurs.
     */

    public int available() throws IOException {
        return wrapped.available();
    }
}