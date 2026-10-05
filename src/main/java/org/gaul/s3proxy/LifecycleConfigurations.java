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

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

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

    /** A child element that may repeat rather than appear at most once. */
    private static final int MANY = Integer.MAX_VALUE;

    /**
     * The elements each container holds, and how often each may appear.  An
     * element named here as a child but not as a container holds text only.
     * Binding alone would not do: Jackson drops an element it does not know
     * and keeps the last of a repeated one, so a Filter with two Prefixes,
     * or one whose condition is misspelled, would bind to a broader rule
     * than the client sent -- one S3 refuses as MalformedXML, and one whose
     * expiration would delete what the client meant to keep.  Kept in step
     * with the records in LifecycleConfigurationRequest: an element named
     * there but not here is refused, which fails closed.
     */
    private static final Map<String, Map<String, Integer>> SCHEMA = Map.of(
            "LifecycleConfiguration", Map.of("Rule", MANY),
            "Rule", Map.of(
                    "ID", 1,
                    "Prefix", 1,
                    "Filter", 1,
                    "Status", 1,
                    "Expiration", 1,
                    "Transition", MANY,
                    "NoncurrentVersionExpiration", 1,
                    "NoncurrentVersionTransition", MANY,
                    "AbortIncompleteMultipartUpload", 1),
            "Filter", Map.of(
                    "Prefix", 1,
                    "Tag", 1,
                    "ObjectSizeGreaterThan", 1,
                    "ObjectSizeLessThan", 1,
                    "And", 1),
            "And", Map.of(
                    "Prefix", 1,
                    "Tag", MANY,
                    "ObjectSizeGreaterThan", 1,
                    "ObjectSizeLessThan", 1),
            "Tag", Map.of("Key", 1, "Value", 1),
            "Expiration", Map.of(
                    "Date", 1, "Days", 1, "ExpiredObjectDeleteMarker", 1),
            "Transition", Map.of("Date", 1, "Days", 1, "StorageClass", 1),
            "NoncurrentVersionExpiration", Map.of(
                    "NoncurrentDays", 1, "NewerNoncurrentVersions", 1),
            "NoncurrentVersionTransition", Map.of(
                    "NoncurrentDays", 1, "NewerNoncurrentVersions", 1,
                    "StorageClass", 1),
            "AbortIncompleteMultipartUpload", Map.of(
                    "DaysAfterInitiation", 1));

    private static final XMLInputFactory INPUT_FACTORY = newInputFactory();
    private static final Pattern NUMBER = Pattern.compile("[+-]?[0-9]+");

    private LifecycleConfigurations() {
    }

    private static XMLInputFactory newInputFactory() {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(
                XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        return factory;
    }

    /**
     * Refuses a body whose elements S3's schema does not allow where they
     * stand: an unknown element, one that appears more often than it may,
     * an element inside one that holds text, text inside one that holds
     * elements, or an attribute on any element.  The root is matched without
     * regard to case, as AWS's own examples spell it LifeCycleConfiguration.
     */
    static void checkStructure(byte[] body) {
        try {
            XMLStreamReader reader = INPUT_FACTORY.createXMLStreamReader(
                    new ByteArrayInputStream(body));
            try {
                checkStructure(reader);
            } finally {
                reader.close();
            }
        } catch (XMLStreamException xse) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L, xse);
        }
    }

    private static void checkStructure(XMLStreamReader reader)
            throws XMLStreamException {
        // Each open element, with how often each child has appeared in it
        // and which child came last.
        Deque<String> names = new ArrayDeque<>();
        Deque<Map<String, Integer>> seen = new ArrayDeque<>();
        Deque<String> last = new ArrayDeque<>();
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                String name = reader.getLocalName();
                if (names.isEmpty()) {
                    if (!name.equalsIgnoreCase("LifecycleConfiguration")) {
                        throw new S3ProxyException(
                                S3ErrorCode.MALFORMED_X_M_L);
                    }
                    name = "LifecycleConfiguration";
                } else {
                    var allowed = SCHEMA.get(names.peek());
                    Integer limit = allowed == null ? null : allowed.get(name);
                    if (limit == null) {
                        throw new S3ProxyException(
                                S3ErrorCode.MALFORMED_X_M_L);
                    }
                    int count = seen.peek().merge(name, 1, Integer::sum);
                    if (count > limit) {
                        throw new S3ProxyException(
                                S3ErrorCode.MALFORMED_X_M_L);
                    }
                    // Jackson keeps only the last run of a repeated element:
                    // Tags split by a Prefix, or Transitions split by a
                    // Status, would lose every copy before the split -- a
                    // dropped tag condition broadens what the rule expires.
                    // The SDKs write each run whole, so a split one is
                    // refused rather than merged.
                    if (count > 1 && !name.equals(last.peek())) {
                        throw new S3ProxyException(
                                S3ErrorCode.MALFORMED_X_M_L);
                    }
                    last.pop();
                    last.push(name);
                }
                // S3's schema gives no lifecycle element an attribute.  Most
                // would only be ignored, but Jackson binds xsi:nil as a
                // missing element -- a nil Prefix or And as an empty filter
                // scoping the rule to the whole bucket.  Namespace
                // declarations are not attributes here, so the root's
                // xmlns survives this.
                if (reader.getAttributeCount() != 0) {
                    throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
                }
                names.push(name);
                seen.push(new HashMap<>());
                last.push("");
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                names.pop();
                seen.pop();
                last.pop();
            } else if ((event == XMLStreamConstants.CHARACTERS ||
                    event == XMLStreamConstants.CDATA) &&
                    !names.isEmpty() && SCHEMA.containsKey(names.peek()) &&
                    !reader.isWhiteSpace() &&
                    !reader.getText().isBlank()) {
                throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
            }
        }
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
            if (rule.id() != null && !rule.id().isEmpty()) {
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

    /**
     * A tag names both a key and a value, as S3's schema requires: one sent
     * without a value is refused rather than given an empty one, which
     * would match a different set of objects.
     */
    private static Tag toTag(LifecycleConfigurationRequest.@Nullable Tag tag) {
        if (tag == null || tag.key() == null || tag.value() == null) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        return Tag.builder()
                .key(tag.key())
                .value(tag.value())
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
            // xs:boolean, whose lexical forms include 1 and 0.
            boolean value = switch (marker.trim()) {
            case "true", "1" -> true;
            case "false", "0" -> false;
            default -> throw new S3ProxyException(
                    S3ErrorCode.MALFORMED_X_M_L);
            };
            return LifecycleExpiration.builder()
                    .expiredObjectDeleteMarker(value)
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
    private static Instant midnight(String text) {
        // xs:dateTime collapses surrounding whitespace.
        String value = text.trim();
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
        try {
            return Integer.parseInt(digits(value));
        } catch (NumberFormatException nfe) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L, nfe);
        }
    }

    private static long size(String value) {
        long parsed;
        try {
            parsed = Long.parseLong(digits(value));
        } catch (NumberFormatException nfe) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L, nfe);
        }
        if (parsed < 0) {
            throw new S3ProxyException(S3ErrorCode.INVALID_ARGUMENT,
                    "Object size must be a nonnegative number");
        }
        return parsed;
    }

    /**
     * A number as xs:int and xs:long spell it: a sign and ASCII digits,
     * with surrounding whitespace collapsed.  Java's parsers alone would
     * also read the digits of every other script.
     */
    private static String digits(@Nullable String value) {
        if (value == null) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        String trimmed = value.trim();
        if (!NUMBER.matcher(trimmed).matches()) {
            throw new S3ProxyException(S3ErrorCode.MALFORMED_X_M_L);
        }
        return trimmed;
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
