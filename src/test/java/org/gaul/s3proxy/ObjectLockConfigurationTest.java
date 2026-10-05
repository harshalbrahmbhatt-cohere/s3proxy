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

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.io.ByteSource;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.gaul.s3proxy.auth.AuthenticationType;
import org.gaul.s3proxy.blobstore.BlobStore;
import org.gaul.s3proxy.blobstore.ForwardingBlobStore;
import org.gaul.s3proxy.blobstore.domain.MultipartUpload;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectLockEnabled;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.ObjectLockMode;
import software.amazon.awssdk.services.s3.model.ObjectLockRetentionMode;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldResponse;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.Md5Utils;

/**
 * What the proxy hands a store with object lock, and what it refuses
 * before the store is asked.  The token is the service's to judge, so it
 * passes through unexamined; an event hold, which the proxy does not pass
 * through, is refused rather than dropped; a body that is not the document
 * it claims to be is MalformedXML rather than read as asking for less.  No
 * emulator says what it received, so a store that records the request
 * stands in for one, and runs on every lane.
 */
public final class ObjectLockConfigurationTest {
    private static final String IDENTITY = "identity";
    private static final String CREDENTIAL = "credential";
    private static final String KEY = "object";
    private static final String XSI =
            "\"http://www.w3.org/2001/XMLSchema-instance\"";

    private S3Proxy s3Proxy;
    private LockRecorder blobStore;
    private S3Client client;
    private String containerName;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    @BeforeEach
    public void setUp() throws Exception {
        blobStore = new LockRecorder(TestUtils.createTransientBlobStore());
        containerName = TestUtils.createRandomContainerName();
        blobStore.createContainer(containerName);
        TestUtils.putBlob(blobStore, containerName, KEY,
                ByteSource.wrap(new byte[1]));
        blobStore.lastPut = null;

        s3Proxy = S3Proxy.builder()
                .stopTimeout(0)
                .blobStore(blobStore)
                .awsAuthentication(AuthenticationType.AWS_V2_OR_V4, IDENTITY,
                        CREDENTIAL)
                .endpoint(URI.create("http://127.0.0.1:0"))
                .build();
        s3Proxy.start();
        while (!s3Proxy.getState().equals("STARTED")) {
            Thread.sleep(10);
        }
        client = client(s3Proxy.getPort());
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (s3Proxy != null) {
            s3Proxy.stop();
        }
    }

    @Test
    public void testTokenPassesThrough() {
        client.putObjectLockConfiguration(b -> b.bucket(containerName)
                .token("token")
                .objectLockConfiguration(c -> c
                        .objectLockEnabled(ObjectLockEnabled.ENABLED)));
        assertThat(blobStore.lastRequest.token()).isEqualTo("token");
    }

    @Test
    public void testNoTokenSendsNone() {
        client.putObjectLockConfiguration(b -> b.bucket(containerName)
                .objectLockConfiguration(c -> c
                        .objectLockEnabled(ObjectLockEnabled.ENABLED)));
        assertThat(blobStore.lastRequest.token()).isNull();
    }

    @Test
    public void testDefaultEventHoldRefused() {
        String body = "<ObjectLockConfiguration" +
                " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
                "<ObjectLockEnabled>Enabled</ObjectLockEnabled>" +
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode>" +
                "<Days>1</Days><DefaultEventHold><Days>5</Days>" +
                "</DefaultEventHold></DefaultRetention></Rule>" +
                "</ObjectLockConfiguration>";
        try (S3Client raw = rawBodyClient(body)) {
            assertThatThrownBy(() -> raw.putObjectLockConfiguration(
                    b -> b.bucket(containerName).objectLockConfiguration(c ->
                            c.objectLockEnabled(ObjectLockEnabled.ENABLED))))
                    .isInstanceOf(S3Exception.class)
                    .extracting(e -> ((S3Exception) e).statusCode())
                    .isEqualTo(501);
        }
        assertThat(blobStore.lastRequest).isNull();
    }

    /**
     * Read past, an event hold would leave an empty Retention, which asks
     * the service to remove retention rather than place the hold.
     */
    @Test
    public void testEventHoldRetentionRefused() {
        String body = "<Retention" +
                " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
                "<EventHold>ON</EventHold><EventHoldDuration>" +
                "<Days>30</Days></EventHoldDuration></Retention>";
        try (S3Client raw = rawBodyClient(body)) {
            assertThatThrownBy(() -> raw.putObjectRetention(b -> b
                    .bucket(containerName).key("object")
                    .bypassGovernanceRetention(true)
                    .retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
                            .retainUntilDate(Instant.now().plusSeconds(60)))))
                    .isInstanceOf(S3Exception.class)
                    .extracting(e -> ((S3Exception) e).statusCode())
                    .isEqualTo(501);
        }
        assertThat(blobStore.lastRetentionRequest).isNull();
    }

    /**
     * Read past, a misnamed element, a different root or a repeated one
     * would leave an empty Retention, which removes retention, or keep the
     * last of two modes.  S3 answers MalformedXML, and so does the proxy.
     */
    @Test
    public void testMalformedRetentionRefused() {
        String until = Instant.now().plusSeconds(3600).toString();
        String[] bodies = {
            "<Retention><mode>GOVERNANCE</mode>" +
                    "<retainUntilDate>" + until + "</retainUntilDate>" +
                    "</Retention>",
            "<LegalHold><Mode>GOVERNANCE</Mode>" +
                    "<RetainUntilDate>" + until + "</RetainUntilDate>" +
                    "</LegalHold>",
            "<Retention><Mode>COMPLIANCE</Mode><Mode>GOVERNANCE</Mode>" +
                    "<RetainUntilDate>" + until + "</RetainUntilDate>" +
                    "</Retention>",
            "<Retention xmlns:xsi=" + XSI + "><Mode xsi:nil=\"true\"/>" +
                    "<RetainUntilDate xsi:nil=\"true\"/></Retention>",
            "<Retention Mode=\"GOVERNANCE\" RetainUntilDate=\"" + until +
                    "\"/>",
            "<Retention><Mode>GOVERNANCE<X/></Mode>" +
                    "<RetainUntilDate>" + until + "</RetainUntilDate>" +
                    "</Retention>",
            "<Retention/><Retention><Mode>GOVERNANCE</Mode></Retention>",
            "<Retention></Retention>junk<"};
        for (String body : bodies) {
            try (S3Client raw = rawBodyClient(body)) {
                assertStatus(() -> raw.putObjectRetention(b -> b
                        .bucket(containerName).key(KEY)
                        .bypassGovernanceRetention(true)
                        .retention(r -> r
                                .mode(ObjectLockRetentionMode.GOVERNANCE)
                                .retainUntilDate(Instant.now()))), 400);
            }
        }
        assertThat(blobStore.lastRetentionRequest).isNull();
    }

    /**
     * An empty Retention is how retention is removed, so it is passed on,
     * however the empty document is spelled.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "<Retention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Retention" +
                " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
                "</Retention>"})
    public void testEmptyRetentionPassesThrough(String body) {
        try (S3Client raw = rawBodyClient(body)) {
            raw.putObjectRetention(b -> b.bucket(containerName).key(KEY)
                    .bypassGovernanceRetention(true)
                    .retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
                            .retainUntilDate(Instant.now())));
        }
        assertThat(blobStore.lastRetentionRequest.retention().mode())
                .isNull();
        assertThat(blobStore.lastRetentionRequest.bypassGovernanceRetention())
                .isTrue();
    }

    @Test
    public void testMalformedLegalHoldRefused() {
        String[] bodies = {
            "<LegalHold><status>ON</status></LegalHold>",
            "<Retention><Status>ON</Status></Retention>",
            "<LegalHold><Status>ON</Status><Status>OFF</Status>" +
                    "</LegalHold>"};
        for (String body : bodies) {
            try (S3Client raw = rawBodyClient(body)) {
                assertStatus(() -> raw.putObjectLegalHold(b -> b
                        .bucket(containerName).key(KEY)
                        .legalHold(h -> h.status(
                                ObjectLockLegalHoldStatus.ON))), 400);
            }
        }
        assertThat(blobStore.lastLegalHoldRequest).isNull();
    }

    /**
     * A misnamed Rule, read past, would be sent on as Enabled with no rule,
     * which removes the bucket's default retention.
     */
    @Test
    public void testMalformedConfigurationRefused() {
        String[] bodies = {
            "<ObjectLockConfiguration>" +
                    "<ObjectLockEnabled>Enabled</ObjectLockEnabled>" +
                    "<rule><DefaultRetention><Mode>COMPLIANCE</Mode>" +
                    "<Days>1</Days></DefaultRetention></rule>" +
                    "</ObjectLockConfiguration>",
            "<ObjectLockConfiguration>" +
                    "<ObjectLockEnabled>Disabled</ObjectLockEnabled>" +
                    "</ObjectLockConfiguration>",
            "<ObjectLockConfiguration>" +
                    "<ObjectLockEnabled>Enabled</ObjectLockEnabled>" +
                    "<Rule/></ObjectLockConfiguration>",
            "<ObjectLockConfiguration xmlns:xsi=" + XSI + ">" +
                    "<ObjectLockEnabled>Enabled</ObjectLockEnabled>" +
                    "<Rule xsi:nil=\"true\"><DefaultRetention>" +
                    "<Mode>COMPLIANCE</Mode><Days>30</Days>" +
                    "</DefaultRetention></Rule></ObjectLockConfiguration>",
            "<ObjectLockConfiguration>" +
                    "<ObjectLockEnabled>Enabled</ObjectLockEnabled>" +
                    "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode>" +
                    "<Days/><Years>1</Years></DefaultRetention></Rule>" +
                    "</ObjectLockConfiguration>"};
        for (String body : bodies) {
            try (S3Client raw = rawBodyClient(body)) {
                assertStatus(() -> raw.putObjectLockConfiguration(b -> b
                        .bucket(containerName).objectLockConfiguration(c -> c
                                .objectLockEnabled(
                                        ObjectLockEnabled.ENABLED))), 400);
            }
        }
        assertThat(blobStore.lastRequest).isNull();
    }

    @Test
    public void testNonPositivePeriodRefused() {
        assertThatThrownBy(() -> client.putObjectLockConfiguration(b -> b
                .bucket(containerName).objectLockConfiguration(c -> c
                        .objectLockEnabled(ObjectLockEnabled.ENABLED)
                        .rule(r -> r.defaultRetention(d -> d
                                .mode(ObjectLockRetentionMode.GOVERNANCE)
                                .days(0))))))
                .isInstanceOf(S3Exception.class)
                .extracting(e -> ((S3Exception) e).awsErrorDetails()
                        .errorCode())
                .isEqualTo("InvalidRetentionPeriod");
        assertThat(blobStore.lastRequest).isNull();
    }

    /**
     * Event hold headers are refused by name, so the refusal holds when
     * ignore-unknown-headers turns off the check that would otherwise
     * catch them.
     */
    @Test
    public void testEventHoldHeaderRefusedIgnoringUnknownHeaders()
            throws Exception {
        S3Proxy lenient = S3Proxy.builder()
                .stopTimeout(0)
                .blobStore(blobStore)
                .awsAuthentication(AuthenticationType.AWS_V2_OR_V4, IDENTITY,
                        CREDENTIAL)
                .ignoreUnknownHeaders(true)
                .endpoint(URI.create("http://127.0.0.1:0"))
                .build();
        lenient.start();
        try {
            while (!lenient.getState().equals("STARTED")) {
                Thread.sleep(10);
            }
            try (S3Client lenientClient = client(lenient.getPort())) {
                assertStatus(() -> lenientClient.putObject(b -> b
                        .bucket(containerName).key("held")
                        .objectLockEventHold("ON")
                        .objectLockEventHoldDurationDays(30),
                        RequestBody.fromString("body")), 501);
            }
        } finally {
            lenient.stop();
        }
        assertThat(blobStore.blobExists(containerName, "held")).isFalse();
    }

    /**
     * The lock a write asks for reaches the store on each of the three
     * writes that can carry one, so the service, which enforces it, sees
     * it.  A copy takes only the lock it names, not the source's.
     */
    @Test
    public void testWritesCarryLockToStore() {
        Instant until = Instant.now().plusSeconds(3600)
                .truncatedTo(ChronoUnit.SECONDS);
        client.putObject(b -> b.bucket(containerName).key("put")
                .objectLockMode(ObjectLockMode.GOVERNANCE)
                .objectLockRetainUntilDate(until)
                .objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON),
                RequestBody.fromString("body"));
        assertThat(blobStore.lastPut.objectLockMode())
                .isEqualTo(ObjectLockMode.GOVERNANCE);
        assertThat(blobStore.lastPut.objectLockRetainUntilDate())
                .isEqualTo(until);
        assertThat(blobStore.lastPut.objectLockLegalHoldStatus())
                .isEqualTo(ObjectLockLegalHoldStatus.ON);

        client.copyObject(b -> b.sourceBucket(containerName).sourceKey(KEY)
                .destinationBucket(containerName).destinationKey("copy")
                .objectLockMode(ObjectLockMode.COMPLIANCE)
                .objectLockRetainUntilDate(until));
        assertThat(blobStore.lastCopy.objectLockMode())
                .isEqualTo(ObjectLockMode.COMPLIANCE);
        assertThat(blobStore.lastCopy.objectLockRetainUntilDate())
                .isEqualTo(until);
        assertThat(blobStore.lastCopy.objectLockLegalHoldStatus()).isNull();

        client.createMultipartUpload(b -> b.bucket(containerName)
                .key("mpu")
                .objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON));
        assertThat(blobStore.lastInitiate.objectLockLegalHoldStatus())
                .isEqualTo(ObjectLockLegalHoldStatus.ON);
        assertThat(blobStore.lastInitiate.objectLockMode()).isNull();
    }

    /**
     * A subresource sent where dispatch does not look for it is refused,
     * rather than answered as the plain operation: a PUT storing the body
     * as the object, a DELETE removing it, a part upload or a copy.  No
     * store is consulted, so the transient store shows it as well as any.
     */
    @Test
    public void testStraySubresourceRefused() throws Exception {
        String path = "/" + containerName + "/" + KEY;
        for (String method : new String[] {"PUT", "DELETE"}) {
            assertThat(signedRequest(method, path, "object-lock")
                    .statusCode()).isEqualTo(501);
        }
        String bucket = "/" + containerName;
        for (String parameter : new String[] {"retention", "legal-hold"}) {
            assertThat(signedRequest("DELETE", bucket, parameter)
                    .statusCode()).isEqualTo(501);
            assertThat(signedRequest("PUT", path,
                    parameter + "&uploadId=upload&partNumber=1")
                    .statusCode()).isEqualTo(501);
        }
        assertThat(blobStore.blobExists(containerName, KEY)).isTrue();
        assertThat(blobStore.containerExists(containerName)).isTrue();
        assertThat(blobStore.lastPut).isNull();
    }

    @Test
    public void testBypassReachesSingleAndMultiDelete() {
        client.deleteObject(b -> b.bucket(containerName).key(KEY)
                .bypassGovernanceRetention(true));
        assertThat(blobStore.lastDeleteBypass).isTrue();
        client.deleteObjects(b -> b.bucket(containerName)
                .bypassGovernanceRetention(true)
                .delete(d -> d.objects(ObjectIdentifier.builder()
                        .key(KEY).build())));
        assertThat(blobStore.lastMultiDeleteBypass).isTrue();
    }

    /**
     * Retention and legal hold have no delete, and a DELETE naming either
     * would otherwise fall through to deleting the object.
     */
    @Test
    public void testDeleteWithLockSubresourceRefused() throws Exception {
        for (String parameter : new String[] {"retention", "legal-hold"}) {
            HttpResponse<String> response = signedRequest("DELETE",
                    "/" + containerName + "/" + KEY, parameter);
            assertThat(response.statusCode()).isEqualTo(501);
        }
        assertThat(blobStore.blobExists(containerName, KEY)).isTrue();
    }

    /**
     * Unsigned callers are refused the lock subresources and headers, and a
     * public read reports the object without its lock, as S3 shows the lock
     * only to a caller allowed GetObjectRetention or GetObjectLegalHold.
     */
    @Test
    public void testUnsignedCallerSeesNoLock() throws Exception {
        blobStore.setContainerAccess(containerName,
                BucketCannedACL.PUBLIC_READ_WRITE);
        blobStore.setBlobAccess(containerName, KEY,
                ObjectCannedACL.PUBLIC_READ);
        String path = "/" + containerName + "/" + KEY;
        for (String parameter : new String[] {"retention", "legal-hold"}) {
            assertThat(unsigned("GET", path + "?" + parameter, null, null)
                    .statusCode()).isEqualTo(403);
        }
        assertThat(unsigned("PUT", "/" + containerName + "/other",
                AwsHttpHeaders.OBJECT_LOCK_LEGAL_HOLD, "ON").statusCode())
                .isEqualTo(403);
        assertThat(blobStore.blobExists(containerName, "other")).isFalse();

        HttpResponse<String> head = unsigned("HEAD", path, null, null);
        assertThat(head.statusCode()).isEqualTo(200);
        assertThat(head.headers().firstValue(
                AwsHttpHeaders.OBJECT_LOCK_MODE)).isEmpty();
        assertThat(head.headers().firstValue(
                AwsHttpHeaders.OBJECT_LOCK_LEGAL_HOLD)).isEmpty();
        // A signed caller is shown it.
        assertThat(client.headObject(b -> b.bucket(containerName).key(KEY))
                .objectLockLegalHoldStatus()).isEqualTo(
                ObjectLockLegalHoldStatus.ON);
    }

    private static void assertStatus(ThrowingCallable call, int status) {
        assertThatThrownBy(call)
                .isInstanceOf(S3Exception.class)
                .extracting(e -> ((S3Exception) e).statusCode())
                .isEqualTo(status);
    }

    private S3Client client(int port) {
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(IDENTITY, CREDENTIAL)))
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://127.0.0.1:" + port))
                .forcePathStyle(true)
                .build();
    }

    private HttpResponse<String> unsigned(String method, String path,
            @Nullable String header, @Nullable String value)
            throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(
                "http://127.0.0.1:" + s3Proxy.getPort() + path))
                .method(method, method.equals("PUT") ?
                        HttpRequest.BodyPublishers.ofString("body") :
                        HttpRequest.BodyPublishers.noBody());
        if (header != null) {
            builder.header(header, value);
        }
        return httpClient.send(builder.build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /**
     * A V2-signed request carrying the given query, as no SDK sends it.  The
     * string to sign names the parameters V2 clients sign, so retention and
     * legal-hold are left out of it and the rest kept, in order.
     */
    private HttpResponse<String> signedRequest(String method, String path,
            String query) throws Exception {
        String date = DateTimeFormatter.RFC_1123_DATE_TIME.format(
                ZonedDateTime.now(ZoneOffset.UTC));
        String signed = Arrays.stream(query.split("&"))
                .filter(p -> !p.equals("retention") &&
                        !p.equals("legal-hold"))
                .sorted()
                .collect(Collectors.joining("&"));
        String stringToSign = String.join("\n", method, "", "", date,
                signed.isEmpty() ? path : path + "?" + signed);
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(new SecretKeySpec(CREDENTIAL.getBytes(
                StandardCharsets.UTF_8), "HmacSHA1"));
        String signature = Base64.getEncoder().encodeToString(mac.doFinal(
                stringToSign.getBytes(StandardCharsets.UTF_8)));
        return httpClient.send(HttpRequest.newBuilder(URI.create(
                "http://127.0.0.1:" + s3Proxy.getPort() + path + "?" +
                        query))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .header("Date", date)
                .header("Authorization", "AWS " + IDENTITY + ":" + signature)
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * A client whose requests send {@code body} in place of the one it
     * built, so the test names the exact XML the proxy receives.
     */
    private S3Client rawBodyClient(String body) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(IDENTITY, CREDENTIAL)))
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(
                        "http://127.0.0.1:" + s3Proxy.getPort()))
                .forcePathStyle(true)
                .overrideConfiguration(o -> o.addExecutionInterceptor(
                        new ExecutionInterceptor() {
                            @Override
                            public SdkHttpRequest modifyHttpRequest(
                                    Context.ModifyHttpRequest context,
                                    ExecutionAttributes attributes) {
                                var request = context.httpRequest()
                                        .toBuilder()
                                        .putHeader("Content-Length",
                                                String.valueOf(bytes.length))
                                        .putHeader("Content-MD5",
                                                Base64.getEncoder()
                                                        .encodeToString(Md5Utils
                                                        .computeMD5Hash(bytes)));
                                for (String name : context.httpRequest()
                                        .headers().keySet()) {
                                    if (name.startsWith("x-amz-checksum-") ||
                                            name.equals(
                                            "x-amz-sdk-checksum-algorithm")) {
                                        request.removeHeader(name);
                                    }
                                }
                                return request.build();
                            }

                            @Override
                            public java.util.Optional<RequestBody>
                                    modifyHttpContent(
                                    Context.ModifyHttpRequest context,
                                    ExecutionAttributes attributes) {
                                return java.util.Optional.of(
                                        RequestBody.fromBytes(bytes));
                            }
                        }))
                .build();
    }

    /** Claims object lock and records the requests it is asked for. */
    private static final class LockRecorder extends ForwardingBlobStore {
        @Nullable private PutObjectLockConfigurationRequest lastRequest;
        @Nullable private PutObjectRetentionRequest lastRetentionRequest;
        @Nullable private PutObjectLegalHoldRequest lastLegalHoldRequest;
        @Nullable private Boolean lastDeleteBypass;
        @Nullable private Boolean lastMultiDeleteBypass;
        @Nullable private PutObjectRequest lastPut;
        @Nullable private CopyObjectRequest lastCopy;
        @Nullable private CreateMultipartUploadRequest lastInitiate;

        LockRecorder(BlobStore blobStore) {
            super(blobStore);
        }

        @Override
        public boolean supportsObjectLock() {
            return true;
        }

        @Override
        public PutObjectLockConfigurationResponse putObjectLockConfiguration(
                PutObjectLockConfigurationRequest request) {
            lastRequest = request;
            return PutObjectLockConfigurationResponse.builder().build();
        }

        @Override
        public PutObjectRetentionResponse putObjectRetention(
                PutObjectRetentionRequest request) {
            lastRetentionRequest = request;
            return PutObjectRetentionResponse.builder().build();
        }

        @Override
        public PutObjectLegalHoldResponse putObjectLegalHold(
                PutObjectLegalHoldRequest request) {
            lastLegalHoldRequest = request;
            return PutObjectLegalHoldResponse.builder().build();
        }

        @Override
        public DeleteObjectResponse removeBlob(DeleteObjectRequest request) {
            lastDeleteBypass = request.bypassGovernanceRetention();
            return super.removeBlob(request);
        }

        @Override
        public DeleteObjectsResponse removeBlobs(
                DeleteObjectsRequest request) {
            lastMultiDeleteBypass = request.bypassGovernanceRetention();
            return super.removeBlobs(request);
        }

        @Override
        public PutObjectResponse putBlob(PutObjectRequest request,
                InputStream payload) {
            lastPut = request;
            return super.putBlob(request, payload);
        }

        @Override
        public CopyObjectResponse copyBlob(CopyObjectRequest request) {
            lastCopy = request;
            return super.copyBlob(request);
        }

        @Override
        public MultipartUpload initiateMultipartUpload(
                CreateMultipartUploadRequest request) {
            lastInitiate = request;
            return super.initiateMultipartUpload(request);
        }

        /** Every object reads as held, which only a signed caller sees. */
        @Override
        public HeadObjectResponse blobMetadata(HeadObjectRequest request) {
            return super.blobMetadata(request).toBuilder()
                    .objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON)
                    .objectLockMode(ObjectLockMode.GOVERNANCE)
                    .build();
        }
    }
}
