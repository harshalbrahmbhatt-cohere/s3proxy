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
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.gaul.s3proxy.auth.AuthenticationType;
import org.gaul.s3proxy.blobstore.BlobStore;
import org.gaul.s3proxy.blobstore.ForwardingBlobStore;
import org.gaul.s3proxy.blobstore.MD5;
import org.gaul.s3proxy.blobstore.S3Exceptions;
import org.gaul.s3proxy.blobstore.domain.MultipartUpload;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.BucketLifecycleConfiguration;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleResponse;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.NoncurrentVersionTransition;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Transition;
import software.amazon.awssdk.services.s3.model.TransitionDefaultMinimumObjectSize;
import software.amazon.awssdk.services.s3.model.TransitionStorageClass;

/**
 * The frontend's side of the ?lifecycle subresource, against a store that
 * keeps configurations the way a pass-through backend's service would: the
 * rules a client writes reach the store vetted and whole, what the store
 * holds reads back as the SDK expects to parse it, and a store that cannot
 * honor lifecycle rules refuses them rather than dropping them.
 */
public final class BucketLifecycleTest {
    private static final String XMLNS =
            "http://s3.amazonaws.com/doc/2006-03-01/";
    private static final String EXPIRATION =
            "expiry-date=\"Fri, 23 Dec 2012 00:00:00 GMT\", rule-id=\"logs\"";

    private BlobStore blobStore;
    private S3Proxy s3Proxy;
    private S3Client client;
    private String containerName;

    private void start(BlobStore store) throws Exception {
        blobStore = store;
        containerName = TestUtils.createRandomContainerName();
        blobStore.createContainer(containerName);
        s3Proxy = S3Proxy.builder()
                .stopTimeout(0)
                .endpoint(URI.create("http://127.0.0.1:0"))
                .blobStore(blobStore)
                .build();
        s3Proxy.start();
        client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("identity", "credential")))
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(
                        "http://127.0.0.1:" + s3Proxy.getPort()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }

    private LifecycleBlobStore startWithLifecycle() throws Exception {
        var store = new LifecycleBlobStore(
                TestUtils.createTransientBlobStore());
        start(store);
        return store;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (s3Proxy != null) {
            s3Proxy.stop();
        }
        if (blobStore != null) {
            blobStore.close();
        }
    }

    @Test
    public void testRoundTripsEveryRuleShape() throws Exception {
        LifecycleBlobStore store = startWithLifecycle();
        var date = Instant.parse("2030-01-01T00:00:00Z");
        List<LifecycleRule> rules = List.of(
                LifecycleRule.builder()
                        .id("logs")
                        .status(ExpirationStatus.ENABLED)
                        .filter(f -> f.prefix("logs/"))
                        .expiration(e -> e.days(30))
                        .transitions(Transition.builder()
                                .days(7)
                                .storageClass(TransitionStorageClass.GLACIER)
                                .build())
                        .noncurrentVersionExpiration(n -> n.noncurrentDays(10)
                                .newerNoncurrentVersions(3))
                        .noncurrentVersionTransitions(
                                NoncurrentVersionTransition.builder()
                                        .noncurrentDays(5)
                                        .storageClass(TransitionStorageClass
                                                .STANDARD_IA)
                                        .build())
                        .abortIncompleteMultipartUpload(
                                a -> a.daysAfterInitiation(2))
                        .build(),
                LifecycleRule.builder()
                        .id("tagged")
                        .status(ExpirationStatus.DISABLED)
                        .filter(f -> f.and(a -> a.prefix("tmp/")
                                .tags(Tag.builder().key("k").value("v")
                                        .build(),
                                        Tag.builder().key("k2").value("")
                                                .build())
                                .objectSizeGreaterThan(1L)
                                .objectSizeLessThan(1_000L)))
                        .expiration(e -> e.date(date))
                        .build(),
                LifecycleRule.builder()
                        .id("markers")
                        .status(ExpirationStatus.ENABLED)
                        .filter(f -> f.tag(t -> t.key("a").value("b")))
                        .expiration(e -> e.expiredObjectDeleteMarker(true))
                        .build(),
                LifecycleRule.builder()
                        .id("everything")
                        .status(ExpirationStatus.ENABLED)
                        .filter(f -> f.objectSizeLessThan(10L))
                        .expiration(e -> e.days(1))
                        .build());

        client.putBucketLifecycleConfiguration(b -> b
                .bucket(containerName)
                .lifecycleConfiguration(c -> c.rules(rules)));

        PutBucketLifecycleConfigurationRequest recorded = store.lastPut;
        assertThat(recorded).isNotNull();
        assertThat(recorded.lifecycleConfiguration().rules())
                .isEqualTo(rules);

        var response = client.getBucketLifecycleConfiguration(
                b -> b.bucket(containerName));
        assertThat(response.rules()).isEqualTo(rules);
    }

    @Test
    public void testDeprecatedRulePrefixRoundTrips() throws Exception {
        startWithLifecycle();
        String body = "<LifecycleConfiguration xmlns=\"" + XMLNS + "\">" +
                "<Rule><ID>old</ID><Prefix>a/</Prefix>" +
                "<Status>Enabled</Status>" +
                "<Expiration><Days>1</Days></Expiration></Rule>" +
                "</LifecycleConfiguration>";
        assertThat(putRaw(body, /*withMd5=*/ true).statusCode())
                .isEqualTo(200);

        HttpResponse<String> get = send(HttpRequest.newBuilder(
                URI.create(lifecycleUrl())).GET());
        assertThat(get.statusCode()).isEqualTo(200);
        assertThat(get.body()).contains("<Prefix>a/</Prefix>")
                .doesNotContain("<Filter>");
    }

    @Test
    public void testEmptyFilterMatchesEverything() throws Exception {
        LifecycleBlobStore store = startWithLifecycle();
        String body = "<LifecycleConfiguration xmlns=\"" + XMLNS + "\">" +
                "<Rule><ID>all</ID><Filter/><Status>Enabled</Status>" +
                "<Expiration><Days>1</Days></Expiration></Rule>" +
                "</LifecycleConfiguration>";
        assertThat(putRaw(body, /*withMd5=*/ true).statusCode())
                .isEqualTo(200);
        LifecycleRule rule = store.lastPut.lifecycleConfiguration().rules()
                .get(0);
        assertThat(rule.filter()).isNotNull();
        assertThat(rule.filter().prefix()).isNull();
        assertThat(rule.filter().and()).isNull();

        HttpResponse<String> get = send(HttpRequest.newBuilder(
                URI.create(lifecycleUrl())).GET());
        assertThat(get.body()).contains("<Filter/>");
    }

    @Test
    public void testMissingConfigurationAnswersNoSuchLifecycleConfiguration()
            throws Exception {
        startWithLifecycle();
        assertThatThrownBy(() -> client.getBucketLifecycleConfiguration(
                b -> b.bucket(containerName)))
                .isInstanceOfSatisfying(S3Exception.class, e -> {
                    assertThat(e.statusCode()).isEqualTo(404);
                    assertThat(e.awsErrorDetails().errorCode())
                            .isEqualTo("NoSuchLifecycleConfiguration");
                });
    }

    @Test
    public void testDeleteRemovesOnlyTheConfiguration() throws Exception {
        startWithLifecycle();
        client.putBucketLifecycleConfiguration(b -> b
                .bucket(containerName)
                .lifecycleConfiguration(c -> c.rules(expireAfter("r", 1))));

        client.deleteBucketLifecycle(b -> b.bucket(containerName));
        // idempotent, as S3's is
        client.deleteBucketLifecycle(b -> b.bucket(containerName));

        assertThat(blobStore.containerExists(containerName)).isTrue();
        assertThatThrownBy(() -> client.getBucketLifecycleConfiguration(
                b -> b.bucket(containerName)))
                .isInstanceOf(S3Exception.class);
    }

    @Test
    public void testTransitionMinimumObjectSizeTravelsBothWays()
            throws Exception {
        LifecycleBlobStore store = startWithLifecycle();
        var put = client.putBucketLifecycleConfiguration(b -> b
                .bucket(containerName)
                .transitionDefaultMinimumObjectSize(
                        TransitionDefaultMinimumObjectSize.VARIES_BY_STORAGE_CLASS)
                .lifecycleConfiguration(c -> c.rules(expireAfter("r", 1))));
        assertThat(store.lastPut.transitionDefaultMinimumObjectSize())
                .isEqualTo(TransitionDefaultMinimumObjectSize
                        .VARIES_BY_STORAGE_CLASS);
        assertThat(put.transitionDefaultMinimumObjectSize())
                .isEqualTo(TransitionDefaultMinimumObjectSize
                        .VARIES_BY_STORAGE_CLASS);

        var get = client.getBucketLifecycleConfiguration(
                b -> b.bucket(containerName));
        assertThat(get.transitionDefaultMinimumObjectSize())
                .isEqualTo(TransitionDefaultMinimumObjectSize
                        .VARIES_BY_STORAGE_CLASS);
    }

    @Test
    public void testPutWithoutChecksumIsRefused() throws Exception {
        LifecycleBlobStore store = startWithLifecycle();
        HttpResponse<String> response = putRaw(
                configuration(rule("r", "<Days>1</Days>")),
                /*withMd5=*/ false);
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("InvalidRequest");
        assertThat(store.lastPut).isNull();
    }

    @Test
    public void testPutWithWrongChecksumIsRefused() throws Exception {
        LifecycleBlobStore store = startWithLifecycle();
        HttpResponse<String> response = send(HttpRequest.newBuilder(
                URI.create(lifecycleUrl()))
                .header("Content-MD5", md5("something else"))
                .PUT(HttpRequest.BodyPublishers.ofString(
                        configuration(rule("r", "<Days>1</Days>")))));
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("BadDigest");
        assertThat(store.lastPut).isNull();
    }

    @Test
    public void testInvalidConfigurationsAreRefused() throws Exception {
        LifecycleBlobStore store = startWithLifecycle();
        Map<String, String> cases = Map.ofEntries(
                // no rules at all
                Map.entry(configuration(""), "MalformedXML"),
                // an ID used twice
                Map.entry(configuration(rule("r", "<Days>1</Days>") +
                        rule("r", "<Days>2</Days>")), "InvalidArgument"),
                // zero and negative days
                Map.entry(configuration(rule("r", "<Days>0</Days>")),
                        "InvalidArgument"),
                Map.entry(configuration(rule("r", "<Days>-1</Days>")),
                        "InvalidArgument"),
                Map.entry(configuration(rule("r", "<Days>x</Days>")),
                        "MalformedXML"),
                // a date that is not midnight UTC
                Map.entry(configuration(rule("r",
                        "<Date>2030-01-01T12:00:00Z</Date>")),
                        "InvalidArgument"),
                // both a date and days
                Map.entry(configuration(rule("r",
                        "<Date>2030-01-01T00:00:00Z</Date><Days>1</Days>")),
                        "MalformedXML"),
                // a delete-marker expiration alongside days
                Map.entry(configuration(rule("r", "<Days>1</Days>" +
                        "<ExpiredObjectDeleteMarker>true" +
                        "</ExpiredObjectDeleteMarker>")), "InvalidArgument"),
                // a status S3 does not have
                Map.entry(configuration("<Rule><ID>r</ID><Filter/>" +
                        "<Status>On</Status><Expiration><Days>1</Days>" +
                        "</Expiration></Rule>"), "MalformedXML"),
                // a rule without an action
                Map.entry(configuration("<Rule><ID>r</ID><Filter/>" +
                        "<Status>Enabled</Status></Rule>"), "InvalidRequest"),
                // two conditions outside an And
                Map.entry(configuration("<Rule><ID>r</ID><Filter>" +
                        "<Prefix>a</Prefix><ObjectSizeLessThan>1" +
                        "</ObjectSizeLessThan></Filter>" +
                        "<Status>Enabled</Status><Expiration><Days>1" +
                        "</Days></Expiration></Rule>"), "MalformedXML"),
                // the rule-level prefix and a filter together
                Map.entry(configuration("<Rule><ID>r</ID><Prefix>a</Prefix>" +
                        "<Filter/><Status>Enabled</Status><Expiration>" +
                        "<Days>1</Days></Expiration></Rule>"),
                        "MalformedXML"),
                // an ID longer than S3 allows
                Map.entry(configuration(rule("r".repeat(256),
                        "<Days>1</Days>")), "InvalidArgument"),
                Map.entry("not xml", "MalformedXML"));
        for (var entry : cases.entrySet()) {
            HttpResponse<String> response = putRaw(entry.getKey(),
                    /*withMd5=*/ true);
            assertThat(response.statusCode())
                    .as(entry.getKey())
                    .isEqualTo(400);
            assertThat(response.body())
                    .as(entry.getKey())
                    .contains("<Code>" + entry.getValue() + "</Code>");
        }
        assertThat(store.lastPut).isNull();
    }

    @Test
    public void testTooManyRulesAreRefused() throws Exception {
        LifecycleBlobStore store = startWithLifecycle();
        var rules = new StringBuilder();
        for (int i = 0; i <= LifecycleConfigurations.MAX_RULES; i++) {
            rules.append(rule("r" + i, "<Days>1</Days>"));
        }
        HttpResponse<String> response = putRaw(configuration(
                rules.toString()), /*withMd5=*/ true);
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("InvalidRequest");
        assertThat(store.lastPut).isNull();
    }

    @Test
    public void testStoreWithoutLifecycleRefusesEveryOperation()
            throws Exception {
        start(TestUtils.createTransientBlobStore());
        assertRefused(() -> client.getBucketLifecycleConfiguration(
                b -> b.bucket(containerName)));
        assertRefused(() -> client.putBucketLifecycleConfiguration(b -> b
                .bucket(containerName)
                .lifecycleConfiguration(c -> c.rules(expireAfter("r", 1)))));
        assertRefused(() -> client.deleteBucketLifecycle(
                b -> b.bucket(containerName)));
        // A refused delete must not fall through to DeleteBucket.
        assertThat(blobStore.containerExists(containerName)).isTrue();
    }

    @Test
    public void testObjectKeyIsNotALifecycleTarget() throws Exception {
        LifecycleBlobStore store = startWithLifecycle();
        client.putObject(b -> b.bucket(containerName).key("key"),
                RequestBody.fromString("original"));
        String objectUrl = "http://127.0.0.1:" + s3Proxy.getPort() + "/" +
                containerName + "/key?lifecycle";
        String body = configuration(rule("r", "<Days>1</Days>"));

        // Neither a PUT nor a DELETE falls through to the object: the PUT
        // would store the configuration as the object, the DELETE remove it.
        HttpResponse<String> put = send(HttpRequest.newBuilder(
                URI.create(objectUrl))
                .header("Content-MD5", md5(body))
                .PUT(HttpRequest.BodyPublishers.ofString(body)));
        assertThat(put.statusCode()).isEqualTo(501);
        HttpResponse<String> delete = send(HttpRequest.newBuilder(
                URI.create(objectUrl)).DELETE());
        assertThat(delete.statusCode()).isEqualTo(501);

        assertThat(store.lastPut).isNull();
        assertThat(client.getObjectAsBytes(b -> b.bucket(containerName)
                .key("key")).asUtf8String()).isEqualTo("original");
    }

    @Test
    public void testAnonymousReadIsRefused() throws Exception {
        startWithLifecycle();
        blobStore.setContainerAccess(containerName,
                BucketCannedACL.PUBLIC_READ);
        s3Proxy.stop();
        s3Proxy = S3Proxy.builder()
                .stopTimeout(0)
                .endpoint(URI.create("http://127.0.0.1:0"))
                .awsAuthentication(AuthenticationType.AWS_V2_OR_V4,
                        "identity", "credential")
                .blobStore(blobStore)
                .build();
        s3Proxy.start();

        HttpResponse<String> response = send(HttpRequest.newBuilder(
                URI.create(lifecycleUrl())).GET());
        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(response.body()).contains("AccessDenied");
    }

    @Test
    public void testRelaysTheStoresExpiration() throws Exception {
        startWithLifecycle();
        PutObjectResponse put = client.putObject(b -> b
                .bucket(containerName).key("logs/a"),
                RequestBody.fromString("hello"));
        assertThat(put.expiration()).isEqualTo(EXPIRATION);

        HeadObjectResponse head = client.headObject(b -> b
                .bucket(containerName).key("logs/a"));
        assertThat(head.expiration()).isEqualTo(EXPIRATION);

        try (ResponseInputStream<GetObjectResponse> get = client.getObject(
                b -> b.bucket(containerName).key("logs/a"))) {
            assertThat(get.response().expiration()).isEqualTo(EXPIRATION);
        }

        var copy = client.copyObject(b -> b
                .sourceBucket(containerName).sourceKey("logs/a")
                .destinationBucket(containerName).destinationKey("logs/b"));
        assertThat(copy.expiration()).isEqualTo(EXPIRATION);

        var upload = client.createMultipartUpload(b -> b
                .bucket(containerName).key("logs/mpu"));
        var part = client.uploadPart(b -> b.bucket(containerName)
                .key("logs/mpu").uploadId(upload.uploadId()).partNumber(1),
                RequestBody.fromString("hello"));
        var completed = client.completeMultipartUpload(b -> b
                .bucket(containerName).key("logs/mpu")
                .uploadId(upload.uploadId())
                .multipartUpload(m -> m.parts(CompletedPart.builder()
                        .partNumber(1)
                        .eTag(part.eTag())
                        .build())));
        assertThat(completed.expiration()).isEqualTo(EXPIRATION);

        // A key no rule matches carries no header, as on S3.
        client.putObject(b -> b.bucket(containerName).key("other"),
                RequestBody.fromString("hello"));
        assertThat(client.headObject(b -> b.bucket(containerName)
                .key("other")).expiration()).isNull();
    }

    private static List<LifecycleRule> expireAfter(String id, int days) {
        return List.of(LifecycleRule.builder()
                .id(id)
                .status(ExpirationStatus.ENABLED)
                .filter(f -> f.prefix(""))
                .expiration(e -> e.days(days))
                .build());
    }

    private static String rule(String id, String expiration) {
        return "<Rule><ID>" + id + "</ID><Filter><Prefix></Prefix></Filter>" +
                "<Status>Enabled</Status><Expiration>" + expiration +
                "</Expiration></Rule>";
    }

    private static String configuration(String rules) {
        return "<LifecycleConfiguration xmlns=\"" + XMLNS + "\">" + rules +
                "</LifecycleConfiguration>";
    }

    private String lifecycleUrl() {
        return "http://127.0.0.1:" + s3Proxy.getPort() + "/" +
                containerName + "?lifecycle";
    }

    private HttpResponse<String> putRaw(String body, boolean withMd5)
            throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(lifecycleUrl()))
                .PUT(HttpRequest.BodyPublishers.ofString(body));
        if (withMd5) {
            builder.header("Content-MD5", md5(body));
        }
        return send(builder);
    }

    private static String md5(String body) {
        return Base64.getEncoder().encodeToString(
                MD5.hash(body.getBytes(StandardCharsets.UTF_8)));
    }

    private static HttpResponse<String> send(HttpRequest.Builder builder)
            throws Exception {
        return HttpClient.newHttpClient().send(builder.build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static void assertRefused(ThrowingCallable callable) {
        assertThatThrownBy(callable)
                .isInstanceOfSatisfying(S3Exception.class, e -> {
                    assertThat(e.statusCode()).isEqualTo(501);
                    assertThat(e.awsErrorDetails().errorCode())
                            .isEqualTo("NotImplemented");
                });
    }

    /**
     * Keeps configurations as a pass-through backend's service would, and
     * reports an expiration on every object under logs/ the way S3 reports
     * one on an object a rule matches.
     */
    private static final class LifecycleBlobStore extends ForwardingBlobStore {
        private final Map<String, GetBucketLifecycleConfigurationResponse>
                configurations = new ConcurrentHashMap<>();
        @Nullable
        private volatile PutBucketLifecycleConfigurationRequest lastPut;

        LifecycleBlobStore(BlobStore delegate) {
            super(delegate);
        }

        @Override
        public boolean supportsBucketLifecycle() {
            return true;
        }

        @Override
        public GetBucketLifecycleConfigurationResponse
                getBucketLifecycleConfiguration(
                        GetBucketLifecycleConfigurationRequest request) {
            var configuration = configurations.get(request.bucket());
            if (configuration == null) {
                throw S3Exceptions.noSuchLifecycleConfiguration(
                        request.bucket());
            }
            return configuration;
        }

        @Override
        public PutBucketLifecycleConfigurationResponse
                putBucketLifecycleConfiguration(
                        PutBucketLifecycleConfigurationRequest request) {
            lastPut = request;
            BucketLifecycleConfiguration configuration =
                    request.lifecycleConfiguration();
            configurations.put(request.bucket(),
                    GetBucketLifecycleConfigurationResponse.builder()
                            .rules(configuration.rules())
                            .transitionDefaultMinimumObjectSize(request
                                    .transitionDefaultMinimumObjectSizeAsString())
                            .build());
            return PutBucketLifecycleConfigurationResponse.builder()
                    .transitionDefaultMinimumObjectSize(request
                            .transitionDefaultMinimumObjectSizeAsString())
                    .build();
        }

        @Override
        public DeleteBucketLifecycleResponse deleteBucketLifecycle(
                DeleteBucketLifecycleRequest request) {
            configurations.remove(request.bucket());
            return DeleteBucketLifecycleResponse.builder().build();
        }

        @Nullable
        private static String expiration(String key) {
            return key.startsWith("logs/") ? EXPIRATION : null;
        }

        @Override
        public PutObjectResponse putBlob(PutObjectRequest request,
                InputStream is) {
            return super.putBlob(request, is).toBuilder()
                    .expiration(expiration(request.key()))
                    .build();
        }

        @Override
        public HeadObjectResponse blobMetadata(HeadObjectRequest request) {
            return super.blobMetadata(request).toBuilder()
                    .expiration(expiration(request.key()))
                    .build();
        }

        @Override
        public ResponseInputStream<GetObjectResponse> getBlob(
                GetObjectRequest request) {
            var blob = super.getBlob(request);
            return new ResponseInputStream<>(blob.response().toBuilder()
                    .expiration(expiration(request.key()))
                    .build(), blob);
        }

        @Override
        public CompleteMultipartUploadResponse completeMultipartUpload(
                MultipartUpload mpu, CompleteMultipartUploadRequest request) {
            return super.completeMultipartUpload(mpu, request).toBuilder()
                    .expiration(expiration(request.key()))
                    .build();
        }

        @Override
        public CopyObjectResponse copyBlob(CopyObjectRequest request) {
            return super.copyBlob(request).toBuilder()
                    .expiration(expiration(request.destinationKey()))
                    .build();
        }
    }
}
