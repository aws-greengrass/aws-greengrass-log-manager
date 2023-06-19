/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigUtilTest {
    private final Context context = new Context();

    @AfterEach()
    void after() {
        context.shutdown();
    }

    @Test
    public void update_from_map_updates_when_changes_exist() {
        Topics root = Topics.of(context, "a", null);
        AtomicInteger callbackCount = new AtomicInteger();
        root.subscribe((w, n) -> {
            callbackCount.incrementAndGet();
        });

        Map<String, Object> map1 = Utils.immutableMap("B", 1, "C", 2);
        long now = System.currentTimeMillis();
        root.updateFromMap(map1, new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now));
        context.waitForPublishQueueToClear();

        assertEquals(5, callbackCount.get());

        ConfigUtil.updateFromMapWhenChanged(root, map1,
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now));

        // Nothing should have changed
        context.waitForPublishQueueToClear();
        assertEquals(5, callbackCount.get());

        Map<String, Object> map2 = Utils.immutableMap("C", 1);
        ConfigUtil.updateFromMapWhenChanged(root, map2,
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now));

        // 2 events to remove B and update the value of C
        context.waitForPublishQueueToClear();
        assertEquals(7, callbackCount.get());

        // Try pushing timestamp forward
        ConfigUtil.updateFromMapWhenChanged(root, map2,
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now+10));

        // Nothing should have changed
        context.waitForPublishQueueToClear();
        assertEquals(7, callbackCount.get());

        // Add in some nesting
        Map<String, Object> map3 = Utils.immutableMap("C", Utils.immutableMap("A", 2));
        ConfigUtil.updateFromMapWhenChanged(root, map3,
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now+10));

        // 4 events more. remove C, add C as Topics, add A, update A
        context.waitForPublishQueueToClear();
        assertEquals(11, callbackCount.get());

        ConfigUtil.updateFromMapWhenChanged(root, map3,
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now+20));

        // Nothing should have changed
        context.waitForPublishQueueToClear();
        assertEquals(11, callbackCount.get());

        Map<String, Object> map4 = Utils.immutableMap("C", Utils.immutableMap("A", 1));
        ConfigUtil.updateFromMapWhenChanged(root, map4,
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now+20));

        // A changed
        context.waitForPublishQueueToClear();
        assertEquals(12, callbackCount.get());
    }
}
