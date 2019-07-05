/*
 * Minio Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2015, 2016, 2017, 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
