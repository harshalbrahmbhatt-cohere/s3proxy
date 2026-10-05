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

import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.gaul.s3proxy.auth.AuthenticationType;
import org.gaul.s3proxy.blobstore.BlobStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.ObjectLockMode;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * A store without object lock refuses every write that asks for one, as
 * before any of it was understood, rather than writing an object nothing
 * protects.  The transient store stands in for every such store.
 */
public final class ObjectLockRefusedTest {
    private static final String IDENTITY = "identity";
    private static final String CREDENTIAL = "credential";

    private S3Proxy s3Proxy;
    private BlobStore blobStore;
    private S3Client client;
    private String containerName;

    @BeforeEach
    public void setUp() throws Exception {
        blobStore = TestUtils.createTransientBlobStore();
        containerName = TestUtils.createRandomContainerName();
        blobStore.createContainer(containerName);

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
        client.putObject(b -> b.bucket(containerName).key("source"),
                RequestBody.fromString("x"));
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

    private static Instant inOneDay() {
        return Instant.now().plus(1, ChronoUnit.DAYS);
    }

    private static void assertNotImplemented(Runnable call) {
        assertThatThrownBy(call::run)
                .isInstanceOf(S3Exception.class)
                .extracting(e -> ((S3Exception) e).statusCode())
                .isEqualTo(501);
    }

    @Test
    public void testPutWithRetentionRefused() {
        assertNotImplemented(() -> client.putObject(b -> b
                .bucket(containerName).key("locked")
                .objectLockMode(ObjectLockMode.GOVERNANCE)
                .objectLockRetainUntilDate(inOneDay()),
                RequestBody.fromString("x")));
        assertThat(blobStore.blobExists(containerName, "locked")).isFalse();
    }

    @Test
    public void testPutWithLegalHoldRefused() {
        assertNotImplemented(() -> client.putObject(b -> b
                .bucket(containerName).key("held")
                .objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON),
                RequestBody.fromString("x")));
        assertThat(blobStore.blobExists(containerName, "held")).isFalse();
    }

    /** A hold OFF asks for no protection, as S3 lets it anywhere. */
    @Test
    public void testPutWithLegalHoldOffAccepted() {
        client.putObject(b -> b.bucket(containerName).key("free")
                .objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.OFF),
                RequestBody.fromString("x"));
        assertThat(blobStore.blobExists(containerName, "free")).isTrue();
    }

    @Test
    public void testCopyWithRetentionRefused() {
        assertNotImplemented(() -> client.copyObject(b -> b
                .sourceBucket(containerName).sourceKey("source")
                .destinationBucket(containerName).destinationKey("copy")
                .objectLockMode(ObjectLockMode.COMPLIANCE)
                .objectLockRetainUntilDate(inOneDay())));
        assertThat(blobStore.blobExists(containerName, "copy")).isFalse();
    }

    @Test
    public void testMultipartWithRetentionRefused() {
        assertNotImplemented(() -> client.createMultipartUpload(b -> b
                .bucket(containerName).key("mpu")
                .objectLockMode(ObjectLockMode.GOVERNANCE)
                .objectLockRetainUntilDate(inOneDay())));
        assertThat(client.listMultipartUploads(b -> b.bucket(containerName))
                .uploads()).isEmpty();
    }

    @Test
    public void testBucketWithObjectLockRefused() {
        String bucket = containerName + "-locked";
        assertNotImplemented(() -> client.createBucket(b -> b.bucket(bucket)
                .objectLockEnabledForBucket(true)));
        assertThat(blobStore.containerExists(bucket)).isFalse();
    }

    @Test
    public void testSubresourcesRefused() {
        assertNotImplemented(() -> client.getObjectLockConfiguration(
                b -> b.bucket(containerName)));
        assertNotImplemented(() -> client.getObjectRetention(
                b -> b.bucket(containerName).key("source")));
        assertNotImplemented(() -> client.getObjectLegalHold(
                b -> b.bucket(containerName).key("source")));
    }
}
