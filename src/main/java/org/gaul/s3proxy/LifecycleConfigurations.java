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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import software.amazon.awssdk.services.s3.model.AbortIncompleteMultipartUpload;
import software.amazon.awssdk.services.s3.model.LifecycleExpiration;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleRuleAndOperator;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.NoncurrentVersionExpiration;
import software.amazon.awssdk.services.s3.model.NoncurrentVersionTransition;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Transition;

/**
 * Turns a PutBucketLifecycleConfiguration body into the SDK's rules, vetting
 * it the way S3 does on the way.  What is refused here is what S3 refuses of
 * any bucket, whatever backs it: the stores see only a configuration S3
 * would have taken, and judge for themselves whether they can carry it out.
 */
final class LifecycleConfigurations {
    /** S3's ceiling on the rules one configuration holds. */
    static final int MAX_RULES = 1_000;
    /** S3's ceiling on a rule ID's length. */
    static final int MAX_ID_LENGTH = 255;

    private LifecycleConfigurations() {
    }

    static List<LifecycleRule> toRules(
            LifecycleConfigurationRequest configuration) {
        var requested = configuration.rules();
        if (requested == null || requested.isEmpty()) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        if (requested.size() > MAX_RULES) {
            throw new S3ProxyException(S3ErrorCode.INVALID_REQUEST,
                    "The number of lifecycle rules must not exceed the" +
                    " allowed limit of " + MAX_RULES + " rules");
        }
        Set<String> ids = new HashSet<>();
        List<LifecycleRule> rules = new ArrayList<>(requested.size());
        for (var rule : requested) {
            if (rule == null) {
                throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
            }
            // S3 names a rule sent without an ID itself; the store passing
            // it through does the same, and one that keeps its own rules
            // is left to choose.
            if (rule.id() != null) {
                if (rule.id().length() > MAX_ID_LENGTH) {
                    throw new S3ProxyException(S3ErrorCode.INVALID_ARGUMENT,
                            "ID length should not exceed allowed limit of " +
                            MAX_ID_LENGTH);
                }
                if (!ids.add(rule.id())) {
                    throw new S3ProxyException(S3ErrorCode.INVALID_ARGUMENT,
                            "Rule ID must be unique. Found same ID for more" +
                            " than one rule");
                }
            }
            rules.add(toRule(rule));
        }
        return rules;
    }

    @SuppressWarnings("deprecation")
    private static LifecycleRule toRule(
            LifecycleConfigurationRequest.Rule rule) {
        String status = rule.status();
        if (!"Enabled".equals(status) && !"Disabled".equals(status)) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        // The deprecated rule-level prefix and the filter are two spellings
        // of one scope, and a rule may use only one of them.
        if (rule.prefix() != null && rule.filter() != null) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        if (rule.expiration() == null &&
                isEmpty(rule.transitions()) &&
                rule.noncurrentVersionExpiration() == null &&
                isEmpty(rule.noncurrentVersionTransitions()) &&
                rule.abortIncompleteMultipartUpload() == null) {
            throw new S3ProxyException(S3ErrorCode.INVALID_REQUEST,
                    "At least one action needs to be specified in a rule");
        }

        var builder = LifecycleRule.builder()
                .id(rule.id())
                .status(status);
        if (rule.prefix() != null) {
            builder.prefix(rule.prefix());
        }
        if (rule.filter() != null) {
            builder.filter(toFilter(rule.filter()));
        }
        if (rule.expiration() != null) {
            builder.expiration(toExpiration(rule.expiration()));
        }
        if (rule.transitions() != null) {
            List<Transition> transitions = new ArrayList<>();
            for (var transition : rule.transitions()) {
                transitions.add(toTransition(transition));
            }
            builder.transitions(transitions);
        }
        var noncurrentExpiration = rule.noncurrentVersionExpiration();
        if (noncurrentExpiration != null) {
            builder.noncurrentVersionExpiration(NoncurrentVersionExpiration
                    .builder()
                    .noncurrentDays(positive(
                            noncurrentExpiration.noncurrentDays(),
                            "NoncurrentDays",
                            "NoncurrentVersionExpiration"))
                    .newerNoncurrentVersions(
                            noncurrentExpiration.newerNoncurrentVersions() ==
                                    null ? null :
                            positive(noncurrentExpiration
                                    .newerNoncurrentVersions(),
                                    "NewerNoncurrentVersions",
                                    "NoncurrentVersionExpiration"))
                    .build());
        }
        if (rule.noncurrentVersionTransitions() != null) {
            List<NoncurrentVersionTransition> transitions = new ArrayList<>();
            for (var transition : rule.noncurrentVersionTransitions()) {
                if (transition == null || transition.storageClass() == null) {
                    throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
                }
                transitions.add(NoncurrentVersionTransition.builder()
                        .noncurrentDays(nonNegative(
                                transition.noncurrentDays(), "NoncurrentDays",
                                "NoncurrentVersionTransition"))
                        .newerNoncurrentVersions(
                                transition.newerNoncurrentVersions() == null ?
                                        null :
                                positive(transition.newerNoncurrentVersions(),
                                        "NewerNoncurrentVersions",
                                        "NoncurrentVersionTransition"))
                        .storageClass(transition.storageClass())
                        .build());
            }
            builder.noncurrentVersionTransitions(transitions);
        }
        var abort = rule.abortIncompleteMultipartUpload();
        if (abort != null) {
            builder.abortIncompleteMultipartUpload(
                    AbortIncompleteMultipartUpload.builder()
                            .daysAfterInitiation(positive(
                                    abort.daysAfterInitiation(),
                                    "DaysAfterInitiation",
                                    "AbortIncompleteMultipartUpload"))
                            .build());
        }
        return builder.build();
    }

    /**
     * A filter holds one condition, or an And holding several.  An empty
     * one is kept as it came: it scopes the rule to every object.
     */
    private static LifecycleRuleFilter toFilter(
            LifecycleConfigurationRequest.Filter filter) {
        int conditions = count(filter.prefix(), filter.tag(),
                filter.objectSizeGreaterThan(), filter.objectSizeLessThan(),
                filter.and());
        if (conditions > 1) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        var builder = LifecycleRuleFilter.builder()
                .prefix(filter.prefix());
        if (filter.tag() != null) {
            builder.tag(toTag(filter.tag()));
        }
        if (filter.objectSizeGreaterThan() != null) {
            builder.objectSizeGreaterThan(size(filter.objectSizeGreaterThan()));
        }
        if (filter.objectSizeLessThan() != null) {
            builder.objectSizeLessThan(size(filter.objectSizeLessThan()));
        }
        var and = filter.and();
        if (and != null) {
            List<Tag> tags = new ArrayList<>();
            if (and.tags() != null) {
                for (var tag : and.tags()) {
                    tags.add(toTag(tag));
                }
            }
            builder.and(LifecycleRuleAndOperator.builder()
                    .prefix(and.prefix())
                    .tags(tags)
                    .objectSizeGreaterThan(and.objectSizeGreaterThan() == null ?
                            null : size(and.objectSizeGreaterThan()))
                    .objectSizeLessThan(and.objectSizeLessThan() == null ?
                            null : size(and.objectSizeLessThan()))
                    .build());
        }
        return builder.build();
    }

    private static Tag toTag(LifecycleConfigurationRequest.@Nullable Tag tag) {
        if (tag == null || tag.key() == null) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        return Tag.builder()
                .key(tag.key())
                .value(tag.value() == null ? "" : tag.value())
                .build();
    }

    /**
     * An expiration names when -- a date or a count of days -- or that a
     * delete marker left with no versions behind it expires, and never more
     * than one of them.
     */
    private static LifecycleExpiration toExpiration(
            LifecycleConfigurationRequest.Expiration expiration) {
        if (expiration.date() != null && expiration.days() != null) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        String marker = expiration.expiredObjectDeleteMarker();
        if (marker != null) {
            if (expiration.date() != null || expiration.days() != null) {
                throw new S3ProxyException(S3ErrorCode.INVALID_ARGUMENT,
                        "ExpiredObjectDeleteMarker cannot be specified with" +
                        " Days or Date in a Lifecycle Expiration Policy");
            }
            if (!marker.equals("true") && !marker.equals("false")) {
                throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
            }
            return LifecycleExpiration.builder()
                    .expiredObjectDeleteMarker(Boolean.valueOf(marker))
                    .build();
        }
        if (expiration.date() != null) {
            return LifecycleExpiration.builder()
                    .date(midnight(expiration.date()))
                    .build();
        }
        if (expiration.days() != null) {
            return LifecycleExpiration.builder()
                    .days(positive(expiration.days(), "Days", "Expiration"))
                    .build();
        }
        throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
    }

    private static Transition toTransition(
            LifecycleConfigurationRequest.@Nullable Transition transition) {
        if (transition == null || transition.storageClass() == null ||
                (transition.date() == null) == (transition.days() == null)) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        var builder = Transition.builder()
                .storageClass(transition.storageClass());
        if (transition.date() != null) {
            builder.date(midnight(transition.date()));
        } else {
            builder.days(nonNegative(transition.days(), "Days",
                    "Transition"));
        }
        return builder.build();
    }

    /**
     * A lifecycle date, which S3 takes only at midnight UTC: its rules act
     * on whole days.  Clients send an ISO 8601 date-time; a bare date is
     * taken as the midnight that begins it.
     */
    private static Instant midnight(String value) {
        Instant instant;
        try {
            instant = OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException dtpe) {
            try {
                instant = LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC)
                        .toInstant();
            } catch (DateTimeParseException dtpe2) {
                throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L, dtpe2);
            }
        }
        if (!instant.atOffset(ZoneOffset.UTC).toLocalTime().equals(
                LocalTime.MIDNIGHT)) {
            throw new S3ProxyException(S3ErrorCode.INVALID_ARGUMENT,
                    "'Date' must be at midnight GMT");
        }
        return instant;
    }

    private static int positive(@Nullable String value, String field,
            String action) {
        int parsed = integer(value);
        if (parsed <= 0) {
            throw new S3ProxyException(S3ErrorCode.INVALID_ARGUMENT,
                    "'" + field + "' for " + action +
                    " action must be a positive integer");
        }
        return parsed;
    }

    private static int nonNegative(@Nullable String value, String field,
            String action) {
        int parsed = integer(value);
        if (parsed < 0) {
            throw new S3ProxyException(S3ErrorCode.INVALID_ARGUMENT,
                    "'" + field + "' for " + action +
                    " action must be a nonnegative integer");
        }
        return parsed;
    }

    private static int integer(@Nullable String value) {
        if (value == null) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException nfe) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L, nfe);
        }
    }

    private static long size(String value) {
        long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException nfe) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L, nfe);
        }
        if (parsed < 0) {
            throw new S3ProxyException(S3ErrorCode.INVALID_ARGUMENT,
                    "Object size must be a nonnegative number");
        }
        return parsed;
    }

    private static boolean isEmpty(@Nullable Iterable<?> values) {
        return values == null || !values.iterator().hasNext();
    }

    private static int count(@Nullable Object... values) {
        int count = 0;
        for (Object value : values) {
            if (value != null) {
                count++;
            }
        }
        return count;
    }
}
