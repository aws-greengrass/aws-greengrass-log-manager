package com.aws.greengrass.logmanager.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ActiveFileCompletedListener {
    public enum ActiveFileStatusType {
        ACTIVE_FILE_COMPLETED
    }

    private final List<Consumer<ActiveFileStatusType>> activeFileUploadedListeners = new ArrayList<>();

    public void registerlistener(Consumer<ActiveFileStatusType> handleActiveFileStatus) {
        activeFileUploadedListeners.add(handleActiveFileStatus);
    }

    public void dispacthStatus(ActiveFileStatusType activeFileStatus) {
        activeFileUploadedListeners.forEach(listener -> listener.accept(activeFileStatus));
    }

}
