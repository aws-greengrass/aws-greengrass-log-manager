/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Hash of two windows of a file at a saved position, up to {@value SPAN} bytes at the head of the file and up to
 * {@value SPAN} bytes just before the position. It tells apart files that share a first line without reading
 * whole files. Bytes outside the two windows are not compared, so files whose windows match pass each other's
 * check even when they differ in between.
 */
public final class PositionCheck {
    static final int SPAN = 512;

    private PositionCheck() {
    }

    /**
     * Computes the check of a file at a position.
     *
     * @param file     the file
     * @param position a position no greater than the file length
     * @return the check
     * @throws IOException if the file cannot be read up to the position
     */
    public static String of(LogFile file, long position) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Every Java platform supports SHA-256", e);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(file.toPath(), StandardOpenOption.READ)) {
            if (position <= 2 * SPAN) {
                digest.update(read(channel, 0, (int) position));
            } else {
                digest.update(read(channel, 0, SPAN));
                digest.update(read(channel, position - SPAN, SPAN));
            }
        }
        return Base64.getEncoder().encodeToString(digest.digest());
    }

    private static ByteBuffer read(SeekableByteChannel channel, long offset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        channel.position(offset);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) < 0) {
                throw new EOFException("File is shorter than the saved position");
            }
        }
        buffer.flip();
        return buffer;
    }
}
