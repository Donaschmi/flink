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

package org.apache.flink.client.cli;

import org.apache.flink.runtime.clusterframework.types.ReschedulePlanJSONMapper;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.cli.CommandLine;

import java.nio.file.Paths;

import static org.apache.flink.client.cli.CliFrontendParser.RESCHEDULE_PLAN_PATH_OPTION;

/** Command line options for the SAVEPOINT command. */
public class RescheduleOptions extends CommandLineOptions {

    private final String[] args;

    private final boolean reschedulePlan;
    private final String reschedulePlanPath;

    public RescheduleOptions(CommandLine line) {
        super(line);
        this.args = line.getArgs();
        this.reschedulePlan = line.hasOption(RESCHEDULE_PLAN_PATH_OPTION.getOpt());
        this.reschedulePlanPath = line.getOptionValue(RESCHEDULE_PLAN_PATH_OPTION.getOpt());
    }

    public String[] getArgs() {
        return args == null ? new String[0] : args;
    }

    public boolean reschedulePlan() {
        return reschedulePlan;
    }

    public String getReschedulePlanPath() {
        return reschedulePlanPath;
    }

    public ReschedulePlanJSONMapper[] getReschedulePlan() throws Exception {
        if (!this.reschedulePlan || this.reschedulePlanPath == null) {
            throw new Exception("ReschedulePlanPath cannot be null.");
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(
                Paths.get(this.reschedulePlanPath).toFile(), ReschedulePlanJSONMapper[].class);
    }
}
