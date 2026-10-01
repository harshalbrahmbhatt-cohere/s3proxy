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
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.gaul.s3proxy.auth.AuthenticationType;
import org.gaul.s3proxy.blobstore.BlobStore;
import org.gaul.s3proxy.blobstore.ForwardingBlobStore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectLockEnabled;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.Md5Utils;

/**
 * What a PutObjectLockConfiguration hands the store.  The token is the
 * service's to judge, so it passes through unexamined; a default event
 * hold, which the SDK the proxy speaks cannot carry, is refused rather
 * than dropped.  No emulator says what it received, so a store that
 * records the request stands in for one.
 */
public final class ObjectLockConfigurationTest {
    private static final String IDENTITY = "identity";
    private static final String CREDENTIAL = "credential";

    private S3Proxy s3Proxy;
    private LockRecorder blobStore;
    private S3Client client;
    private String containerName;

    @BeforeEach
    public void setUp() throws Exception {
        blobStore = new LockRecorder(TestUtils.createTransientBlobStore());
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
     * A client whose PutObjectLockConfiguration sends {@code body} in place
     * of the one it built, which is how an element the SDK has no field
     * for reaches the proxy.
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

    /** Claims object lock and records the configuration it is asked for. */
    private static final class LockRecorder extends ForwardingBlobStore {
        @Nullable private PutObjectLockConfigurationRequest lastRequest;

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
    }
}
