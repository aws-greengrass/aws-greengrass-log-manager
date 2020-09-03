/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.kernel.EvergreenService;


@ImplementsService(name = "aws.greengrass.logManager")
public class LogManagerService extends EvergreenService {
    public LogManagerService(Topics topics) {
        super(topics);
    }
}
