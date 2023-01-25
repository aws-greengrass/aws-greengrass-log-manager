package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.LogManagerService;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 * LRU cache that holds information about each file that is being processed. As long as a log file is in the file system
 * and can be found, we can't know for sure if that file won't get new contents written to it in the future.
 */
public class ProcessingFileLRU {
    @Getter
    @Setter
    private int capacity;
    @Getter
    private int size;
    private Node head;
    private Node tail;
    private final HashMap<String, Node> cache;


    public ProcessingFileLRU(int capacity) {
        this.cache = new HashMap<>();
        this.capacity = capacity;
        this.size = 0;
        this.head = null;
        this.tail = null;
    }

    static class Node {
        private final String fileHash;
        private final LogManagerService.CurrentProcessingFileInformation fileInformation;
        public Node next = null;
        public Node prev = null;


        Node(String fileHash, LogManagerService.CurrentProcessingFileInformation fileInformation) {
            this.fileHash = fileHash;
            this.fileInformation = fileInformation;
        }

        Node(String fileHash, LogManagerService.CurrentProcessingFileInformation fileInformation, Node next) {
            this(fileHash, fileInformation);
            this.next = next;
        }
    }


    public void put(LogManagerService.CurrentProcessingFileInformation info) {
        String fileHash = info.getFileHash();

        if (cache.containsKey(fileHash)) {
            this.detach(cache.get(fileHash));
            this.size--;
        } else if (this.size == this.capacity) {
            this.cache.remove(fileHash);
            this.detach(this.tail);
            this.size--;
        }

        if (this.head == null) {
            this.head = this.tail = new Node(fileHash, info);
        } else {
            Node node = new Node(fileHash, info, this.head);
            this.head.prev = node;
            this.head = node;
        }

        cache.put(fileHash, head);
        this.size++;
    }

    public Optional<LogManagerService.CurrentProcessingFileInformation> get(String fileHash) {
        if (cache.containsKey(fileHash)) {
            Node existingNode = cache.get(fileHash);

            if (this.head != existingNode) {
               this.put(existingNode.fileInformation);
            }

            return Optional.of(existingNode.fileInformation);
        }

        return Optional.empty();
    }

    private void detach(Node node) {
        if (node.prev != null) {
            node.prev.next = node.next;
        } else {
            this.head = node.next;
        }

        if (node.next != null) {
            node.next.prev = node.prev;
        } else {
            this.tail = node.prev;
        }
    }

    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();

        Node node = head;
        while (node != null) {
            System.out.println(node.fileHash);
            map.put(node.fileHash, node.fileInformation.convertToMapOfObjects());
            node = node.next;
        }

        return map;
    }
}
