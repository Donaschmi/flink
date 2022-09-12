/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.job.rescheduling;

import org.apache.flink.runtime.clusterframework.types.ReschedulePlanJSONMapper;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Optional;

/** {@link RequestBody} to trigger rescheduling. */
public class ReschedulingTriggerRequestBody implements RequestBody {

    public static final String FIELD_NAME_TRIGGER_ID = "triggerId";

    public static final String FIELD_NAME_RESCHEDULE_PLAN = "reschedulePlan";

    @JsonProperty(FIELD_NAME_TRIGGER_ID)
    @Nullable
    private final TriggerId triggerId;

    @JsonProperty(FIELD_NAME_RESCHEDULE_PLAN)
    @Nullable
    private final ReschedulePlanJSONMapper reschedulePlan;

    @JsonCreator
    public ReschedulingTriggerRequestBody(
            @Nullable @JsonProperty(FIELD_NAME_TRIGGER_ID) TriggerId triggerId,
            @Nullable @JsonProperty(FIELD_NAME_RESCHEDULE_PLAN)
                    ReschedulePlanJSONMapper reschedulePlan) {
        this.triggerId = triggerId;
        this.reschedulePlan = reschedulePlan;
    }

    @JsonIgnore
    public Optional<TriggerId> getTriggerId() {
        return Optional.ofNullable(triggerId);
    }

    @JsonIgnore
    public ReschedulePlanJSONMapper getReschedulePlan() {
        return reschedulePlan;
    }
}
