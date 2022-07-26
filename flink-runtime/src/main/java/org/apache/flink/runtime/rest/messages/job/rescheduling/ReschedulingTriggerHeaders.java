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

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** These headers define the protocol for rescheduling a job. */
public class ReschedulingTriggerHeaders
        extends AsynchronousOperationTriggerMessageHeaders<
                ReschedulingTriggerRequestBody, ReschedulingTriggerMessageParameters> {

    private static final ReschedulingTriggerHeaders INSTANCE = new ReschedulingTriggerHeaders();

    private static final String URL = String.format("/jobs/:%s/reschedule", JobIDPathParameter.KEY);

    private ReschedulingTriggerHeaders() {}

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    protected String getAsyncOperationDescription() {
        return "Triggers a rescheduling.";
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.ACCEPTED;
    }

    @Override
    public Class<ReschedulingTriggerRequestBody> getRequestClass() {
        return ReschedulingTriggerRequestBody.class;
    }

    @Override
    public ReschedulingTriggerMessageParameters getUnresolvedMessageParameters() {
        return new ReschedulingTriggerMessageParameters();
    }

    public static ReschedulingTriggerHeaders getInstance() {
        return INSTANCE;
    }
}
