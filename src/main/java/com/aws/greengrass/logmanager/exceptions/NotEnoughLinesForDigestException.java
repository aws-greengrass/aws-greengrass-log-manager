package com.aws.greengrass.logmanager.exceptions;

public class NotEnoughLinesForDigestException extends Exception {
    public NotEnoughLinesForDigestException(String message) {
        super(message);
    }
}
