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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.gaul.s3proxy.blobstore.BlobStore;
import org.gaul.s3proxy.blobstore.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectLockEnabled;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.ObjectLockMode;
import software.amazon.awssdk.services.s3.model.ObjectLockRetentionMode;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * S3 object lock against whichever store the conf names: retention and
 * legal hold protect a version, while the key itself stays overwritable
 * and deletable.  A store without object lock answers 501 to the whole
 * family, which is AwsSdkTest's business; this skips it.
 */
public final class ObjectLockTest {
    private static final byte[] FIRST = "first".getBytes(
            StandardCharsets.UTF_8);
    private static final byte[] SECOND = "second".getBytes(
            StandardCharsets.UTF_8);
    /**
     * COMPLIANCE retention cannot be lifted, so it lasts only a few
     * seconds and tearDown waits it out before emptying the bucket.
     */
    private static final Duration COMPLIANCE_PERIOD = Duration.ofSeconds(5);
    /** MiniStack's lane port, as VersionedBlobStoreTest tells it apart. */
    private static final int MINISTACK_PORT = 4567;

    private S3Proxy s3Proxy;
    private BlobStore blobStore;
    private S3Client client;
    /** Appends {@link #strayParameter} to every request it sends. */
    private S3Client strayClient;
    private String strayParameter;
    private String containerName;
    private Instant complianceUntil;
    private TestUtils.S3ProxyLaunchInfo info;

    @BeforeEach
    public void setUp() throws Exception {
        info = TestUtils.startS3Proxy(
                System.getProperty("s3proxy.test.conf", "s3proxy.conf"));
        blobStore = info.getBlobStore();
        s3Proxy = info.getS3Proxy();

        var creds = AwsBasicCredentials.create(info.getS3Identity(),
                info.getS3Credential());
        var attributeMap = AttributeMap.builder()
                .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                .build();
        client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(
                        info.getSecureEndpoint().toString() +
                        info.getServicePath()))
                .httpClient(Apache5HttpClient.builder()
                        .buildWithDefaults(attributeMap))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();

        strayClient = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(
                        info.getSecureEndpoint().toString() +
                        info.getServicePath()))
                .httpClient(Apache5HttpClient.builder()
                        .buildWithDefaults(attributeMap))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .overrideConfiguration(o -> o.addExecutionInterceptor(
                        new ExecutionInterceptor() {
                            @Override
                            public SdkHttpRequest modifyHttpRequest(
                                    Context.ModifyHttpRequest context,
                                    ExecutionAttributes attributes) {
                                return context.httpRequest().toBuilder()
                                        .putRawQueryParameter(
                                                strayParameter, "")
                                        .build();
                            }
                        }))
                .build();

        assumeTrue(blobStore.supportsObjectLock());

        containerName = TestUtils.createRandomContainerName();
        client.createBucket(b -> b.bucket(containerName)
                .objectLockEnabledForBucket(true));
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            if (client != null && containerName != null) {
                emptyLockedBucket();
                client.deleteBucket(b -> b.bucket(containerName));
            }
        } finally {
            if (client != null) {
                client.close();
            }
            if (strayClient != null) {
                strayClient.close();
            }
            if (s3Proxy != null) {
                s3Proxy.stop();
            }
        }
    }

    /**
     * Lifts every hold and bypasses every GOVERNANCE retention, after
     * letting any COMPLIANCE retention run out, so the bucket can go.
     */
    private void emptyLockedBucket() throws InterruptedException {
        if (complianceUntil != null) {
            long wait = Duration.between(Instant.now(), complianceUntil)
                    .toMillis() + 1_000;
            if (wait > 0) {
                Thread.sleep(wait);
            }
        }
        var listing = client.listObjectVersions(
                b -> b.bucket(containerName));
        for (ObjectVersion version : listing.versions()) {
            try {
                client.putObjectLegalHold(b -> b.bucket(containerName)
                        .key(version.key()).versionId(version.versionId())
                        .legalHold(h -> h.status(
                                ObjectLockLegalHoldStatus.OFF)));
            } catch (S3Exception se) {
                // no hold to lift
            }
            client.deleteObject(b -> b.bucket(containerName)
                    .key(version.key()).versionId(version.versionId())
                    .bypassGovernanceRetention(true));
        }
        for (var marker : listing.deleteMarkers()) {
            client.deleteObject(b -> b.bucket(containerName)
                    .key(marker.key()).versionId(marker.versionId()));
        }
    }

    /** Asserts the lock refused the call, which S3 answers 403. */
    private static void assertLockRefused(Runnable call) {
        assertThatThrownBy(call::run)
                .isInstanceOf(S3Exception.class)
                .satisfies(e -> assertThat(lockRefusal((S3Exception) e))
                        .isTrue());
    }

    private static boolean lockRefusal(S3Exception e) {
        return e.statusCode() == 403;
    }

    private boolean versionExists(String key, String versionId) {
        try {
            client.headObject(b -> b.bucket(containerName).key(key)
                    .versionId(versionId));
            return true;
        } catch (S3Exception e) {
            return false;
        }
    }

    private boolean isMiniStack() {
        var properties = info.getProperties();
        return "aws-s3".equals(properties.getProperty(
                Constants.PROPERTY_PROVIDER)) &&
                URI.create(properties.getProperty(Constants.PROPERTY_ENDPOINT,
                        "http://stub")).getPort() == MINISTACK_PORT;
    }

    private static Instant inOneDay() {
        return Instant.now().plus(1, ChronoUnit.DAYS)
                .truncatedTo(ChronoUnit.SECONDS);
    }

    private String putGoverned(String key, byte[] content) {
        return client.putObject(b -> b.bucket(containerName).key(key)
                .objectLockMode(ObjectLockMode.GOVERNANCE)
                .objectLockRetainUntilDate(inOneDay()),
                RequestBody.fromBytes(content)).versionId();
    }

    @Test
    public void testBucketConfigurationEnabled() {
        var configuration = client.getObjectLockConfiguration(
                b -> b.bucket(containerName)).objectLockConfiguration();
        assertThat(configuration.objectLockEnabled())
                .isEqualTo(ObjectLockEnabled.ENABLED);
    }

    @Test
    public void testDefaultRetentionRoundTrips() {
        client.putObjectLockConfiguration(b -> b.bucket(containerName)
                .objectLockConfiguration(c -> c
                        .objectLockEnabled(ObjectLockEnabled.ENABLED)
                        .rule(r -> r.defaultRetention(d -> d
                                .mode(ObjectLockRetentionMode.GOVERNANCE)
                                .days(1)))));
        var rule = client.getObjectLockConfiguration(
                b -> b.bucket(containerName))
                .objectLockConfiguration().rule();
        assertThat(rule.defaultRetention().mode())
                .isEqualTo(ObjectLockRetentionMode.GOVERNANCE);
        assertThat(rule.defaultRetention().days()).isEqualTo(1);

        // Stamped on a write that names no lock of its own.
        client.putObject(b -> b.bucket(containerName).key("defaulted"),
                RequestBody.fromBytes(FIRST));
        HeadObjectResponse head = client.headObject(
                b -> b.bucket(containerName).key("defaulted"));
        assertThat(head.objectLockMode()).isEqualTo(ObjectLockMode.GOVERNANCE);
        assertThat(head.objectLockRetainUntilDate()).isAfter(Instant.now());
    }

    @Test
    public void testDefaultRetentionNeedsExactlyOnePeriod() {
        assertThatThrownBy(() -> client.putObjectLockConfiguration(
                b -> b.bucket(containerName).objectLockConfiguration(c -> c
                        .objectLockEnabled(ObjectLockEnabled.ENABLED)
                        .rule(r -> r.defaultRetention(d -> d
                                .mode(ObjectLockRetentionMode.GOVERNANCE)
                                .days(1).years(1))))))
                .isInstanceOf(S3Exception.class)
                .extracting(e -> ((S3Exception) e).statusCode())
                .isEqualTo(400);
    }

    @Test
    public void testHeadersRoundTripOnReads() {
        Instant until = inOneDay();
        client.putObject(b -> b.bucket(containerName).key("locked")
                .objectLockMode(ObjectLockMode.GOVERNANCE)
                .objectLockRetainUntilDate(until)
                .objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON),
                RequestBody.fromBytes(FIRST));

        HeadObjectResponse head = client.headObject(
                b -> b.bucket(containerName).key("locked"));
        assertThat(head.objectLockMode()).isEqualTo(ObjectLockMode.GOVERNANCE);
        assertThat(head.objectLockRetainUntilDate()).isEqualTo(until);
        assertThat(head.objectLockLegalHoldStatus())
                .isEqualTo(ObjectLockLegalHoldStatus.ON);

        var get = client.getObjectAsBytes(
                b -> b.bucket(containerName).key("locked")).response();
        assertThat(get.objectLockMode()).isEqualTo(ObjectLockMode.GOVERNANCE);
        assertThat(get.objectLockRetainUntilDate()).isEqualTo(until);
        assertThat(get.objectLockLegalHoldStatus())
                .isEqualTo(ObjectLockLegalHoldStatus.ON);
    }

    /** A copy's lock is the destination's, as the request names it. */
    @Test
    public void testCopyCarriesLock() {
        // MiniStack drops the lock a copy or a multipart create names.
        assumeTrue(!isMiniStack());
        client.putObject(b -> b.bucket(containerName).key("source"),
                RequestBody.fromBytes(FIRST));
        Instant until = inOneDay();
        client.copyObject(b -> b.sourceBucket(containerName)
                .sourceKey("source").destinationBucket(containerName)
                .destinationKey("copy")
                .objectLockMode(ObjectLockMode.GOVERNANCE)
                .objectLockRetainUntilDate(until));
        HeadObjectResponse head = client.headObject(
                b -> b.bucket(containerName).key("copy"));
        assertThat(head.objectLockMode()).isEqualTo(ObjectLockMode.GOVERNANCE);
        assertThat(head.objectLockRetainUntilDate()).isEqualTo(until);
        assertThat(client.headObject(b -> b.bucket(containerName)
                .key("source")).objectLockMode()).isNull();
    }

    /** The lock CreateMultipartUpload names lands on the completed object. */
    @Test
    public void testMultipartCarriesLock() {
        // MiniStack drops the lock a copy or a multipart create names.
        assumeTrue(!isMiniStack());
        Instant until = inOneDay();
        String uploadId = client.createMultipartUpload(b -> b
                .bucket(containerName).key("mpu")
                .objectLockMode(ObjectLockMode.GOVERNANCE)
                .objectLockRetainUntilDate(until)
                .objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON))
                .uploadId();
        String eTag = client.uploadPart(b -> b.bucket(containerName)
                .key("mpu").uploadId(uploadId).partNumber(1),
                RequestBody.fromBytes(FIRST)).eTag();
        client.completeMultipartUpload(b -> b.bucket(containerName)
                .key("mpu").uploadId(uploadId)
                .multipartUpload(m -> m.parts(CompletedPart.builder()
                        .partNumber(1).eTag(eTag).build())));
        HeadObjectResponse head = client.headObject(
                b -> b.bucket(containerName).key("mpu"));
        assertThat(head.objectLockMode()).isEqualTo(ObjectLockMode.GOVERNANCE);
        assertThat(head.objectLockRetainUntilDate()).isEqualTo(until);
        assertThat(head.objectLockLegalHoldStatus())
                .isEqualTo(ObjectLockLegalHoldStatus.ON);
    }

    @Test
    public void testModeWithoutDateRefused() {
        assertThatThrownBy(() -> client.putObject(
                b -> b.bucket(containerName).key("half")
                        .objectLockMode(ObjectLockMode.GOVERNANCE),
                RequestBody.fromBytes(FIRST)))
                .isInstanceOf(S3Exception.class)
                .extracting(e -> ((S3Exception) e).statusCode())
                .isEqualTo(400);
    }

    @Test
    public void testOverwriteAndPlainDeleteSucceed() {
        // MiniStack refuses a plain delete of a locked key, where S3 stacks
        // a marker over it.
        assumeTrue(!isMiniStack());
        String first = putGoverned("key", FIRST);
        // A lock protects the version, not the key: a new write stacks a
        // version and a plain delete stacks a marker over both.
        String second = client.putObject(
                b -> b.bucket(containerName).key("key"),
                RequestBody.fromBytes(SECOND)).versionId();
        assertThat(second).isNotEqualTo(first);
        var deleted = client.deleteObject(
                b -> b.bucket(containerName).key("key"));
        assertThat(deleted.deleteMarker()).isTrue();

        assertThat(client.getObjectAsBytes(b -> b.bucket(containerName)
                .key("key").versionId(first)).asByteArray())
                .isEqualTo(FIRST);
    }

    @Test
    public void testGovernanceVersionDeleteNeedsBypass() {
        String versionId = putGoverned("key", FIRST);
        assertLockRefused(() -> client.deleteObject(
                b -> b.bucket(containerName).key("key")
                        .versionId(versionId)));
        assertThat(versionExists("key", versionId)).isTrue();

        client.deleteObject(b -> b.bucket(containerName).key("key")
                .versionId(versionId).bypassGovernanceRetention(true));
        assertThat(versionExists("key", versionId)).isFalse();
    }

    @Test
    public void testLegalHoldBlocksVersionDelete() {
        String versionId = client.putObject(
                b -> b.bucket(containerName).key("held"),
                RequestBody.fromBytes(FIRST)).versionId();
        client.putObjectLegalHold(b -> b.bucket(containerName).key("held")
                .versionId(versionId)
                .legalHold(h -> h.status(ObjectLockLegalHoldStatus.ON)));
        assertThat(client.getObjectLegalHold(b -> b.bucket(containerName)
                .key("held").versionId(versionId)).legalHold().status())
                .isEqualTo(ObjectLockLegalHoldStatus.ON);

        // The bypass lifts retention, never a hold.
        assertLockRefused(() -> client.deleteObject(
                b -> b.bucket(containerName).key("held")
                        .versionId(versionId)
                        .bypassGovernanceRetention(true)));
        assertThat(versionExists("held", versionId)).isTrue();
    }

    @Test
    public void testRetentionRoundTripsAndShorteningNeedsBypass() {
        String versionId = putGoverned("key", FIRST);
        var retention = client.getObjectRetention(b -> b
                .bucket(containerName).key("key").versionId(versionId))
                .retention();
        assertThat(retention.mode())
                .isEqualTo(ObjectLockRetentionMode.GOVERNANCE);

        Instant sooner = Instant.now().plus(1, ChronoUnit.HOURS)
                .truncatedTo(ChronoUnit.SECONDS);
        assertLockRefused(() -> client.putObjectRetention(b -> b
                .bucket(containerName).key("key").versionId(versionId)
                .retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
                        .retainUntilDate(sooner))));

        client.putObjectRetention(b -> b.bucket(containerName).key("key")
                .versionId(versionId).bypassGovernanceRetention(true)
                .retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
                        .retainUntilDate(sooner)));
        assertThat(client.getObjectRetention(b -> b.bucket(containerName)
                .key("key").versionId(versionId)).retention()
                .retainUntilDate()).isEqualTo(sooner);
    }

    @Test
    public void testComplianceCannotBeBypassed() {
        complianceUntil = Instant.now().plus(COMPLIANCE_PERIOD)
                .truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
        String versionId = client.putObject(b -> b.bucket(containerName)
                .key("compliant")
                .objectLockMode(ObjectLockMode.COMPLIANCE)
                .objectLockRetainUntilDate(complianceUntil),
                RequestBody.fromBytes(FIRST)).versionId();
        assertLockRefused(() -> client.deleteObject(
                b -> b.bucket(containerName).key("compliant")
                        .versionId(versionId)
                        .bypassGovernanceRetention(true)));
        assertThat(versionExists("compliant", versionId)).isTrue();
    }

    @Test
    public void testMultiDeleteReportsLockedVersionPerKey() {
        String locked = putGoverned("locked", FIRST);
        String free = client.putObject(
                b -> b.bucket(containerName).key("free"),
                RequestBody.fromBytes(SECOND)).versionId();
        DeleteObjectsResponse result = client.deleteObjects(b -> b
                .bucket(containerName).delete(d -> d.objects(
                        ObjectIdentifier.builder().key("locked")
                                .versionId(locked).build(),
                        ObjectIdentifier.builder().key("free")
                                .versionId(free).build())));
        assertThat(result.deleted()).extracting(o -> o.key())
                .containsExactly("free");
        assertThat(result.errors()).extracting(e -> e.key())
                .containsExactly("locked");
        assertThat(result.errors().get(0).code())
                .isEqualTo("AccessDenied");
        assertThat(versionExists("locked", locked)).isTrue();
    }

    /**
     * A subresource sent to the level it does not describe is refused, not
     * ignored: dispatch would otherwise answer the plain operation, storing
     * the body as the object or deleting the object or the bucket.
     */
    @Test
    public void testSubresourceAtWrongLevelRefused() {
        String versionId = client.putObject(
                b -> b.bucket(containerName).key("key"),
                RequestBody.fromBytes(FIRST)).versionId();

        strayParameter = "object-lock";
        assertThatThrownBy(() -> strayClient.putObject(
                b -> b.bucket(containerName).key("key"),
                RequestBody.fromBytes(SECOND)))
                .satisfies(ObjectLockTest::assertNotImplemented);
        assertThatThrownBy(() -> strayClient.deleteObject(
                b -> b.bucket(containerName).key("key")))
                .satisfies(ObjectLockTest::assertNotImplemented);

        for (String parameter : new String[] {"retention", "legal-hold"}) {
            strayParameter = parameter;
            assertThatThrownBy(() -> strayClient.deleteBucket(
                    b -> b.bucket(containerName)))
                    .satisfies(ObjectLockTest::assertNotImplemented);
            String fresh = containerName + "-" + parameter;
            assertThatThrownBy(() -> strayClient.createBucket(
                    b -> b.bucket(fresh)))
                    .satisfies(ObjectLockTest::assertNotImplemented);
            assertThatThrownBy(() -> client.headBucket(
                    b -> b.bucket(fresh)))
                    .isInstanceOf(S3Exception.class)
                    .extracting(e -> ((S3Exception) e).statusCode())
                    .isEqualTo(404);
        }

        // Nor beside an upload id or a copy source, which would otherwise
        // pick the operation: an abort, a part upload, a copy over the key.
        String uploadId = client.createMultipartUpload(
                b -> b.bucket(containerName).key("key")).uploadId();
        for (String parameter : new String[] {"retention", "legal-hold"}) {
            strayParameter = parameter;
            assertThatThrownBy(() -> strayClient.abortMultipartUpload(
                    b -> b.bucket(containerName).key("key")
                            .uploadId(uploadId)))
                    .satisfies(ObjectLockTest::assertNotImplemented);
            assertThatThrownBy(() -> strayClient.uploadPart(
                    b -> b.bucket(containerName).key("key")
                            .uploadId(uploadId).partNumber(1),
                    RequestBody.fromBytes(SECOND)))
                    .satisfies(ObjectLockTest::assertNotImplemented);
            assertThatThrownBy(() -> strayClient.copyObject(
                    b -> b.sourceBucket(containerName).sourceKey("key")
                            .destinationBucket(containerName)
                            .destinationKey("key")
                            .metadataDirective("REPLACE")))
                    .satisfies(ObjectLockTest::assertNotImplemented);
        }
        assertThat(client.listParts(b -> b.bucket(containerName).key("key")
                .uploadId(uploadId)).parts()).isEmpty();
        client.abortMultipartUpload(b -> b.bucket(containerName).key("key")
                .uploadId(uploadId));

        // The object is untouched: no new version, no delete marker.
        assertThat(client.headObject(b -> b.bucket(containerName)
                .key("key")).versionId()).isEqualTo(versionId);
        assertThat(client.getObjectAsBytes(b -> b.bucket(containerName)
                .key("key")).asByteArray()).isEqualTo(FIRST);
    }

    private static void assertNotImplemented(Throwable thrown) {
        assertThat(thrown).isInstanceOf(S3Exception.class);
        assertThat(((S3Exception) thrown).statusCode()).isEqualTo(501);
    }

    /**
     * A browser POST says in form fields what a PUT says in headers, the
     * lock among them, and the object it writes is locked the same way.
     */
    @Test
    public void testPostObjectFieldsLock() throws Exception {
        Instant until = inOneDay();
        HttpResponse<String> response = postForm("posted",
                "x-amz-object-lock-mode", "GOVERNANCE",
                "x-amz-object-lock-retain-until-date", until.toString(),
                "x-amz-object-lock-legal-hold", "ON");
        assertThat(response.statusCode()).isEqualTo(204);

        HeadObjectResponse head = client.headObject(
                b -> b.bucket(containerName).key("posted"));
        assertThat(head.objectLockMode()).isEqualTo(ObjectLockMode.GOVERNANCE);
        assertThat(head.objectLockRetainUntilDate()).isEqualTo(until);
        assertThat(head.objectLockLegalHoldStatus())
                .isEqualTo(ObjectLockLegalHoldStatus.ON);
        String versionId = head.versionId();
        assertLockRefused(() -> client.deleteObject(
                b -> b.bucket(containerName).key("posted")
                        .versionId(versionId)));
    }

    /**
     * Sends a SigV2-signed browser form whose policy admits exactly the
     * fields given, over the plain endpoint.
     */
    private HttpResponse<String> postForm(String key, String... fields)
            throws Exception {
        var conditions = new StringBuilder("{\"bucket\": \"" +
                containerName + "\"}, {\"key\": \"" + key + "\"}");
        for (int i = 0; i < fields.length; i += 2) {
            conditions.append(", {\"").append(fields[i]).append("\": \"")
                    .append(fields[i + 1]).append("\"}");
        }
        String policy = Base64.getEncoder().encodeToString(
                ("{\"expiration\": \"" +
                Instant.now().plus(1, ChronoUnit.HOURS) +
                "\", \"conditions\": [" + conditions + "]}")
                .getBytes(StandardCharsets.UTF_8));
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(new SecretKeySpec(info.getS3Credential()
                .getBytes(StandardCharsets.UTF_8), "HmacSHA1"));
        String signature = Base64.getEncoder().encodeToString(
                mac.doFinal(policy.getBytes(StandardCharsets.UTF_8)));

        String boundary = "----------s3proxytest";
        var out = new ByteArrayOutputStream();
        var all = new ArrayList<>(List.of("key", key,
                "AWSAccessKeyId", info.getS3Identity(),
                "policy", policy, "signature", signature));
        all.addAll(List.of(fields));
        for (int i = 0; i < all.size(); i += 2) {
            out.write(("--" + boundary + "\r\nContent-Disposition:" +
                    " form-data; name=\"" + all.get(i) + "\"\r\n\r\n" +
                    all.get(i + 1) + "\r\n")
                    .getBytes(StandardCharsets.UTF_8));
        }
        out.write(("--" + boundary + "\r\nContent-Disposition: form-data;" +
                " name=\"file\"; filename=\"payload\"\r\n\r\n" +
                new String(FIRST, StandardCharsets.UTF_8) + "\r\n--" +
                boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));

        String path = info.getServicePath();
        URI uri = URI.create(info.getEndpoint().toString() +
                (path == null ? "" : path) + "/" + containerName);
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(uri)
                .header("Content-Type",
                        "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(
                        out.toByteArray()))
                .build(), HttpResponse.BodyHandlers.ofString());
    }
}
