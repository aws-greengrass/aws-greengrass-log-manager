/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.config.CaseInsensitiveString;
import com.aws.greengrass.config.Node;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UnsupportedInputTypeException;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class ConfigUtil {
    private static final Logger logger = LogManager.getLogger(ConfigUtil.class);

    private ConfigUtil() {
    }

    /**
     * Same as topics.updateFromMap, but only makes the update when the value actually changes, skipping any unnecessary
     * timestampUpdated events. Ideally this code would exist in Topics, but it isn't, so we need to do this in order to
     * maintain compatibility.
     *
     * @param topics    Topics to update with values from the map
     * @param newValues the new value to apply
     * @param ubt       update behavior tree
     */
    public static void updateFromMapWhenChanged(Topics topics, Map<String, Object> newValues, UpdateBehaviorTree ubt) {
        Set<CaseInsensitiveString> childrenToRemove = new HashSet<>(topics.children.keySet());

        newValues.forEach((okey, value) -> {
            CaseInsensitiveString key = new CaseInsensitiveString(okey);
            childrenToRemove.remove(key);
            updateChild(topics, key, value, ubt);
        });

        childrenToRemove.forEach(childName -> {
            UpdateBehaviorTree childMergeBehavior = ubt.getChildBehavior(childName.toString());

            // remove the existing child if its merge behavior is REPLACE
            if (childMergeBehavior.getBehavior() == UpdateBehaviorTree.UpdateBehavior.REPLACE) {
                topics.remove(topics.children.get(childName));
            }
        });
    }

    private static void updateChild(Topics t, CaseInsensitiveString key, Object value,
                                    @NonNull UpdateBehaviorTree mergeBehavior) {
        UpdateBehaviorTree childMergeBehavior = mergeBehavior.getChildBehavior(key.toString());

        Node existingChild = t.children.get(key);
        // if new node is a container node
        if (value instanceof Map) {
            // if existing child is a container node
            if (existingChild == null || existingChild instanceof Topics) {
                updateFromMapWhenChanged(t.createInteriorChild(key.toString()), (Map) value, childMergeBehavior);
            } else {
                t.remove(existingChild);
                Topics newNode = t.createInteriorChild(key.toString(), mergeBehavior.getTimestampToUse());
                updateFromMapWhenChanged(newNode, (Map) value, childMergeBehavior);
            }
            // if new node is a leaf node
        } else {
            try {
                if (existingChild == null || existingChild instanceof Topic) {
                    Topic node = t.createLeafChild(key.toString());
                    if (!Objects.equals(node.getOnce(), value)) {
                        node.withValueChecked(childMergeBehavior.getTimestampToUse(), value);
                    }
                } else {
                    t.remove(existingChild);
                    Topic newNode = t.createLeafChild(key.toString());
                    newNode.withValueChecked(childMergeBehavior.getTimestampToUse(), value);
                }
            } catch (UnsupportedInputTypeException e) {
                logger.error("Should never fail in updateChild", e);
            }
        }
    }
}
