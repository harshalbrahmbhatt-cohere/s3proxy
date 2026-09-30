/*
 * Copyright 2014-2026 Andrew Gaul <andrew@gaul.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gaul.s3proxy;

import java.util.Collection;

import tools.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import tools.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * The PutBucketLifecycleConfiguration body.  Numbers are kept as the strings
 * the client sent, so that one that does not parse is refused as S3 refuses
 * it rather than as whatever Jackson makes of it.
 */
record LifecycleConfigurationRequest(
        @JacksonXmlProperty(localName = "Rule")
        @JacksonXmlElementWrapper(useWrapping = false)
        Collection<Rule> rules) {

    record Rule(
            @JacksonXmlProperty(localName = "ID") String id,
            // The deprecated rule-level spelling of the filter's prefix.
            @JacksonXmlProperty(localName = "Prefix") String prefix,
            @JacksonXmlProperty(localName = "Filter") Filter filter,
            @JacksonXmlProperty(localName = "Status") String status,
            @JacksonXmlProperty(localName = "Expiration")
            Expiration expiration,
            @JacksonXmlProperty(localName = "Transition")
            @JacksonXmlElementWrapper(useWrapping = false)
            Collection<Transition> transitions,
            @JacksonXmlProperty(localName = "NoncurrentVersionExpiration")
            NoncurrentVersionExpiration noncurrentVersionExpiration,
            @JacksonXmlProperty(localName = "NoncurrentVersionTransition")
            @JacksonXmlElementWrapper(useWrapping = false)
            Collection<NoncurrentVersionTransition>
                    noncurrentVersionTransitions,
            @JacksonXmlProperty(localName = "AbortIncompleteMultipartUpload")
            AbortIncompleteMultipartUpload abortIncompleteMultipartUpload) {
    }

    record Filter(
            @JacksonXmlProperty(localName = "Prefix") String prefix,
            @JacksonXmlProperty(localName = "Tag") Tag tag,
            @JacksonXmlProperty(localName = "ObjectSizeGreaterThan")
            String objectSizeGreaterThan,
            @JacksonXmlProperty(localName = "ObjectSizeLessThan")
            String objectSizeLessThan,
            @JacksonXmlProperty(localName = "And") And and) {
    }

    record And(
            @JacksonXmlProperty(localName = "Prefix") String prefix,
            @JacksonXmlProperty(localName = "Tag")
            @JacksonXmlElementWrapper(useWrapping = false)
            Collection<Tag> tags,
            @JacksonXmlProperty(localName = "ObjectSizeGreaterThan")
            String objectSizeGreaterThan,
            @JacksonXmlProperty(localName = "ObjectSizeLessThan")
            String objectSizeLessThan) {
    }

    record Tag(
            @JacksonXmlProperty(localName = "Key") String key,
            @JacksonXmlProperty(localName = "Value") String value) {
    }

    record Expiration(
            @JacksonXmlProperty(localName = "Date") String date,
            @JacksonXmlProperty(localName = "Days") String days,
            @JacksonXmlProperty(localName = "ExpiredObjectDeleteMarker")
            String expiredObjectDeleteMarker) {
    }

    record Transition(
            @JacksonXmlProperty(localName = "Date") String date,
            @JacksonXmlProperty(localName = "Days") String days,
            @JacksonXmlProperty(localName = "StorageClass")
            String storageClass) {
    }

    record NoncurrentVersionExpiration(
            @JacksonXmlProperty(localName = "NoncurrentDays")
            String noncurrentDays,
            @JacksonXmlProperty(localName = "NewerNoncurrentVersions")
            String newerNoncurrentVersions) {
    }

    record NoncurrentVersionTransition(
            @JacksonXmlProperty(localName = "NoncurrentDays")
            String noncurrentDays,
            @JacksonXmlProperty(localName = "NewerNoncurrentVersions")
            String newerNoncurrentVersions,
            @JacksonXmlProperty(localName = "StorageClass")
            String storageClass) {
    }

    record AbortIncompleteMultipartUpload(
            @JacksonXmlProperty(localName = "DaysAfterInitiation")
            String daysAfterInitiation) {
    }
}
