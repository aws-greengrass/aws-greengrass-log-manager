/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import java.io.IOException;
import java.io.Reader;

/**
 * An implementation of BufferedReader which adds a position() method to get an accurate reading of the number of bytes
 * which have been read so far.
 * Reimplement BufferedReader because we cannot override the readline method to properly count the bytes read.
 */
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:JavadocParagraph",
        "checkstyle:JavadocMethod", "checkstyle:VariableDeclarationUsageDistance",
        "PMD.NullAssignment", "PMD.UselessParentheses", "PMD.AvoidBranchingStatementAsLastInLoop"})
public class PositionTrackingBufferedReader extends Reader {
    public static final String NOT_IMPLEMENTED = "Not implemented";
    private long position = 0;

    private Reader in;

    private char[] cb;
    private int nChars;
    private int nextChar;

    /** If the next character is a line feed, skip it. */
    private boolean skipLF = false;
    /** The skipLF flag when the mark was set. */

    private static int defaultCharBufferSize = 8192;
    private static int defaultExpectedLineLength = 80;

    /**
     * Creates a buffering character-input stream that uses an input buffer of
     * the specified size.
     *
     * @param  in   A Reader
     * @param  sz   Input-buffer size
     *
     * @exception  IllegalArgumentException  If {@code sz <= 0}
     */
    public PositionTrackingBufferedReader(Reader in, int sz) {
        super(in);
        if (sz <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        this.in = in;
        cb = new char[sz];
        nextChar = nChars = 0;
    }

    /**
     * Creates a buffering character-input stream that uses a default-sized
     * input buffer.
     *
     * @param  in   A Reader
     */
    public PositionTrackingBufferedReader(Reader in) {
        this(in, defaultCharBufferSize);
    }

    /** Checks to make sure that the stream has not been closed. */
    private void ensureOpen() throws IOException {
        if (in == null) {
            throw new IOException("Stream closed");
        }
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    private void fill() throws IOException {
        int n;
        do {
            n = in.read(cb, 0, cb.length);
        } while (n == 0);
        if (n > 0) {
            nChars = n;
            nextChar = 0;
        }
    }

    /**
     * Reads a line of text.  A line is considered to be terminated by any one
     * of a line feed ('\n'), a carriage return ('\r'), or a carriage return
     * followed immediately by a linefeed.
     *
     * @return     A String containing the contents of the line, not including
     *             any line-termination characters, or null if the end of the
     *             stream has been reached
     *
     * @exception  IOException  If an I/O error occurs
     *
     * @see java.nio.file.Files#readAllLines
     */
    public String readLine() throws IOException {
        StringBuilder s = null;
        int startChar;

        synchronized (lock) {
            ensureOpen();
            boolean omitLF = skipLF;

            while (true) {
                if (nextChar >= nChars) {
                    fill();
                }
                if (nextChar >= nChars) { /* EOF */
                    if (s != null && s.length() > 0) {
                        return s.toString();
                    } else {
                        return null;
                    }
                }
                boolean eol = false;
                char c = 0;
                int i;

                /* Skip a leftover '\n', if necessary */
                if (omitLF && (cb[nextChar] == '\n')) {
                    nextChar++;
                }
                skipLF = false;
                omitLF = false;

                for (i = nextChar; i < nChars; i++) {
                    c = cb[i];
                    // Greengrass-added position tracking
                    position++;
                    if ((c == '\n') || (c == '\r')) {
                        eol = true;
                        break;
                    }
                }

                startChar = nextChar;
                nextChar = i;

                if (eol) {
                    String str;
                    if (s == null) {
                        str = new String(cb, startChar, i - startChar);
                    } else {
                        s.append(cb, startChar, i - startChar);
                        str = s.toString();
                    }
                    nextChar++;
                    if (c == '\r') {
                        skipLF = true;
                        // Greengrass-added position tracking
                        position++;
                    }
                    return str;
                }

                if (s == null) {
                    s = new StringBuilder(defaultExpectedLineLength);
                }
                s.append(cb, startChar, i - startChar);
            }
        }
    }

    @Override
    public long skip(long n) throws IOException {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public boolean ready() throws IOException {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readAheadLimit) throws IOException {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (in == null) {
                return;
            }
            try {
                in.close();
            } finally {
                in = null;
                cb = null;
            }
        }
    }

    public long position() {
        return position;
    }

    /**
     * Set the starting position counter. This doesn't actually change anything in the stream, ie. this method will
     * not skip x bytes.
     *
     * @param startPosition the initial position to begin the counter.
     */
    public void setInitialPosition(long startPosition) {
        position = startPosition;
    }
}
