/*
 * Copyright (c) 2015-2021 MinIO, Inc.
 *
 * This file is part of MinIO Object Storage stack
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.minio.awssdk.tests;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class MintLogger {

    @JsonProperty("name")
    private String name;

    @JsonProperty("function")
    private String function;

    @JsonProperty("args")
    private String args;

    @JsonProperty("duration")
    private long duration;

    @JsonProperty("status")
    private String status;

    @JsonProperty("alert")
    private String alert;

    @JsonProperty("message")
    private String message;

    @JsonProperty("error")
    private String error;

    /**
     * Constructor.
     **/
    public MintLogger(String function,
            String args,
            long duration,
            String status,
            String alert,
            String message,
            String error) {
        this.name = "aws-sdk-java";
        this.function = function;
        this.duration = duration;
        this.args = args;
        this.status = status;
        this.alert = alert;
        this.message = message;
        this.error = error;
    }

    /**
     * Return JSON Log Entry.
     **/
    @JsonIgnore
    public String toString() {

        try {
            return new ObjectMapper().setSerializationInclusion(Include.NON_NULL).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Return Alert.
     **/
    @JsonIgnore
    public String alert() {
        return alert;
    }

    /**
     * Return Error.
     **/
    @JsonIgnore
    public String error() {
        return error;
    }

    /**
     * Return Message.
     **/
    @JsonIgnore
    public String message() {
        return message;
    }

    /**
     * Return args.
     **/
    @JsonIgnore
    public String args() {
        return args;
    }

    /**
     * Return status.
     **/
    @JsonIgnore
    public String status() {
        return status;
    }

    /**
     * Return name.
     **/
    @JsonIgnore
    public String name() {
        return name;
    }

    /**
     * Return function.
     **/
    @JsonIgnore
    public String function() {
        return function;
    }

    /**
     * Return duration.
     **/
    @JsonIgnore
    public long duration() {
        return duration;
    }
}
