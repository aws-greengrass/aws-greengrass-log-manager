package com.aws.greengrass.logmanager.exceptions;

public class InvalidLogGroupException extends Exception {
    // custom serialVersionUID for class extends Serializable class
    private static final long serialVersionUID = 456;

    public InvalidLogGroupException(String s) {
        super(s);
    }

    public InvalidLogGroupException(String s, Exception e) {
        super(s, e);
    }
}
