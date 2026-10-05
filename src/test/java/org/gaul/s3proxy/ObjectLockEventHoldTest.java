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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import com.google.common.io.ByteSource;

import org.gaul.s3proxy.auth.AuthenticationType;
import org.gaul.s3proxy.blobstore.BlobStore;
import org.gaul.s3proxy.blobstore.ForwardingBlobStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.EventHoldDuration;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectLockEnabled;
import software.amazon.awssdk.services.s3.model.ObjectLockEventHold;

/**
 * An event hold placed on the service directly, which a write through the
 * proxy cannot ask for, is still reported by every read that describes a
 * lock: the bucket rule, a version's retention, and the GET and HEAD
 * headers.  No emulator holds one, so a store that reports one stands in.
 */
public final class ObjectLockEventHoldTest {
    private static final String IDENTITY = "identity";
    private static final String CREDENTIAL = "credential";
    private static final String KEY = "object";

    private S3Proxy s3Proxy;
    private BlobStore blobStore;
    private S3Client client;
    private String containerName;

    @BeforeEach
    public void setUp() throws Exception {
        blobStore = new EventHoldReporter(
                TestUtils.createTransientBlobStore());
        containerName = TestUtils.createRandomContainerName();
        blobStore.createContainer(containerName);
        TestUtils.putBlob(blobStore, containerName, KEY,
                ByteSource.wrap(new byte[1]));

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
        client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(IDENTITY, CREDENTIAL)))
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(
                        "http://127.0.0.1:" + s3Proxy.getPort()))
                .forcePathStyle(true)
                .build();
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
    public void testConfigurationReportsDefaultEventHold() {
        var retention = client.getObjectLockConfiguration(
                b -> b.bucket(containerName))
                .objectLockConfiguration().rule().defaultRetention();
        assertThat(retention.defaultEventHold().days()).isEqualTo(30);
        assertThat(retention.defaultEventHold().years()).isNull();
    }

    @Test
    public void testRetentionReportsEventHold() {
        var retention = client.getObjectRetention(
                b -> b.bucket(containerName).key(KEY)).retention();
        assertThat(retention.eventHold()).isEqualTo(ObjectLockEventHold.ON);
        assertThat(retention.eventHoldDuration().years()).isEqualTo(2);
        assertThat(retention.eventHoldDuration().days()).isNull();
    }

    @Test
    public void testHeadReportsEventHold() {
        HeadObjectResponse head = client.headObject(
                b -> b.bucket(containerName).key(KEY));
        assertThat(head.objectLockEventHold())
                .isEqualTo(ObjectLockEventHold.ON);
        assertThat(head.objectLockEventHoldDurationYears()).isEqualTo(2);
        assertThat(head.objectLockEventHoldDurationDays()).isNull();
    }

    /** Like the rest of the lock, a hold is not shown an unsigned caller. */
    @Test
    public void testUnsignedHeadOmitsEventHold() throws Exception {
        blobStore.setContainerAccess(containerName,
                BucketCannedACL.PUBLIC_READ);
        blobStore.setBlobAccess(containerName, KEY,
                ObjectCannedACL.PUBLIC_READ);
        HttpResponse<Void> head = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" +
                        s3Proxy.getPort() + "/" + containerName + "/" + KEY))
                        .method("HEAD", HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.discarding());
        assertThat(head.statusCode()).isEqualTo(200);
        assertThat(head.headers().firstValue(
                AwsHttpHeaders.OBJECT_LOCK_EVENT_HOLD)).isEmpty();
    }

    @Test
    public void testGetReportsEventHold() throws Exception {
        try (ResponseInputStream<GetObjectResponse> get = client.getObject(
                b -> b.bucket(containerName).key(KEY))) {
            assertThat(get.response().objectLockEventHold())
                    .isEqualTo(ObjectLockEventHold.ON);
            assertThat(get.response().objectLockEventHoldDurationYears())
                    .isEqualTo(2);
        }
    }

    /** Claims object lock and reports an event hold on every read. */
    private static final class EventHoldReporter extends ForwardingBlobStore {
        EventHoldReporter(BlobStore blobStore) {
            super(blobStore);
        }

        @Override
        public boolean supportsObjectLock() {
            return true;
        }

        @Override
        public GetObjectLockConfigurationResponse getObjectLockConfiguration(
                GetObjectLockConfigurationRequest request) {
            return GetObjectLockConfigurationResponse.builder()
                    .objectLockConfiguration(c -> c
                            .objectLockEnabled(ObjectLockEnabled.ENABLED)
                            .rule(r -> r.defaultRetention(d -> d
                                    .defaultEventHold(EventHoldDuration
                                            .builder().days(30).build()))))
                    .build();
        }

        @Override
        public GetObjectRetentionResponse getObjectRetention(
                GetObjectRetentionRequest request) {
            return GetObjectRetentionResponse.builder()
                    .retention(r -> r.eventHold(ObjectLockEventHold.ON)
                            .eventHoldDuration(d -> d.years(2)))
                    .build();
        }

        @Override
        public HeadObjectResponse blobMetadata(HeadObjectRequest request) {
            return super.blobMetadata(request).toBuilder()
                    .objectLockEventHold(ObjectLockEventHold.ON)
                    .objectLockEventHoldDurationYears(2)
                    .build();
        }

        @Override
        public ResponseInputStream<GetObjectResponse> getBlob(
                GetObjectRequest request) {
            ResponseInputStream<GetObjectResponse> blob =
                    super.getBlob(request);
            return new ResponseInputStream<>(blob.response().toBuilder()
                    .objectLockEventHold(ObjectLockEventHold.ON)
                    .objectLockEventHoldDurationYears(2)
                    .build(), blob);
        }
    }
}
