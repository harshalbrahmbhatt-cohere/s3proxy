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

package org.gaul.s3proxy.middleware;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableBiMap;

import org.gaul.s3proxy.S3ProxyConstants;
import org.gaul.s3proxy.TestUtils;
import org.gaul.s3proxy.blobstore.BlobStore;
import org.gaul.s3proxy.blobstore.ForwardingBlobStore;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleResponse;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationResponse;

/**
 * What each bucket-renaming or bucket-scoping middleware does with the
 * lifecycle configuration: the alias store sends it to the real bucket, and
 * the stores whose virtual bucket is not one whole backend bucket -- a
 * sharded one, a prefixed one -- refuse it rather than configure a bucket
 * the caller never named, while their other buckets pass through.
 */
public final class LifecycleMiddlewareTest {
    /**
     * A put of one rule for the given bucket.  A fresh builder each time:
     * the SDK's builders are mutable, and these tests run concurrently.
     */
    private static PutBucketLifecycleConfigurationRequest put(String bucket) {
        return PutBucketLifecycleConfigurationRequest.builder()
                .bucket(bucket)
                .lifecycleConfiguration(c -> c.rules(LifecycleRule.builder()
                        .id("r")
                        .status(ExpirationStatus.ENABLED)
                        .filter(f -> f.prefix("logs/"))
                        .expiration(e -> e.days(1))
                        .build()))
                .build();
    }

    @Test
    public void testAliasSendsTheRealBucketName() {
        var recorder = new LifecycleRecorder();
        BlobStore store = AliasBlobStore.newAliasBlobStore(recorder,
                ImmutableBiMap.of("alias", "backend"));

        var unused1 = store.putBucketLifecycleConfiguration(
                put("alias"));
        var unused2 = store.getBucketLifecycleConfiguration(
                GetBucketLifecycleConfigurationRequest.builder()
                        .bucket("alias").build());
        var unused3 = store.deleteBucketLifecycle(
                DeleteBucketLifecycleRequest.builder()
                        .bucket("alias").build());

        assertThat(recorder.buckets).containsExactly(
                "backend", "backend", "backend");
    }

    @Test
    public void testShardedBucketIsRefused() {
        var recorder = new LifecycleRecorder();
        BlobStore store = ShardedBlobStore.newShardedBlobStore(recorder,
                Map.of("sharded", 4), Map.of("sharded", "shard"));

        assertRefusesEveryOperation(store, "sharded");
        assertThat(recorder.buckets).isEmpty();

        var unused = store.putBucketLifecycleConfiguration(
                put("plain"));
        assertThat(recorder.buckets).containsExactly("plain");
    }

    @Test
    public void testPrefixedBucketIsRefused() {
        var recorder = new LifecycleRecorder();
        BlobStore store = PrefixBlobStore.newPrefixBlobStore(recorder,
                Map.of("prefixed", "tenant/"));

        // The rules would govern the whole backend bucket, other tenants'
        // keys included.
        assertRefusesEveryOperation(store, "prefixed");
        assertThat(recorder.buckets).isEmpty();

        var unused = store.putBucketLifecycleConfiguration(
                put("plain"));
        assertThat(recorder.buckets).containsExactly("plain");
    }

    @Test
    public void testReadOnlyRefusesWritesButReads() {
        var recorder = new LifecycleRecorder();
        BlobStore store = ReadOnlyBlobStore.newReadOnlyBlobStore(recorder);

        assertThatThrownBy(() -> store.putBucketLifecycleConfiguration(
                put("b")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.deleteBucketLifecycle(
                DeleteBucketLifecycleRequest.builder().bucket("b").build()))
                .isInstanceOf(UnsupportedOperationException.class);
        var unused = store.getBucketLifecycleConfiguration(
                GetBucketLifecycleConfigurationRequest.builder()
                        .bucket("b").build());
        assertThat(recorder.buckets).containsExactly("b");
    }

    @Test
    public void testRegexStoreDisablesLifecycle() {
        var properties = new Properties();
        properties.setProperty(
                S3ProxyConstants.PROPERTY_REGEX_BLOBSTORE_MATCH + ".rename",
                "^foo/(.*)");
        properties.setProperty(
                S3ProxyConstants.PROPERTY_REGEX_BLOBSTORE_REPLACE + ".rename",
                "bar/$1");
        BlobStore store = RegexBlobStore.newRegexBlobStore(
                new LifecycleRecorder(),
                RegexBlobStore.parseRegexs(properties));
        assertThat(store.supportsBucketLifecycle()).isFalse();
    }

    @Test
    public void testEncryptedStoreDisablesLifecycle() throws Exception {
        var properties = new Properties();
        properties.setProperty(S3ProxyConstants.PROPERTY_ENCRYPTED_BLOBSTORE,
                "true");
        properties.setProperty(
                S3ProxyConstants.PROPERTY_ENCRYPTED_BLOBSTORE_PASSWORD,
                "Password1234567!");
        properties.setProperty(
                S3ProxyConstants.PROPERTY_ENCRYPTED_BLOBSTORE_SALT,
                "12345678");
        BlobStore store = EncryptedBlobStore.newEncryptedBlobStore(
                new LifecycleRecorder(), properties);
        assertThat(store.supportsBucketLifecycle()).isFalse();
    }

    @Test
    public void testNullStoreDisablesLifecycle() {
        BlobStore store = NullBlobStore.newNullBlobStore(
                new LifecycleRecorder());
        assertThat(store.supportsBucketLifecycle()).isFalse();
    }

    @Test
    public void testEventualStoreDisablesLifecycle() {
        var executor = Executors.newSingleThreadScheduledExecutor();
        try {
            BlobStore store = EventualBlobStore.newEventualBlobStore(
                    new LifecycleRecorder(), new LifecycleRecorder(),
                    executor, 0, TimeUnit.SECONDS, 1.0);
            assertThat(store.supportsBucketLifecycle()).isFalse();
        } finally {
            executor.shutdownNow();
        }
    }

    private static void assertRefusesEveryOperation(BlobStore store,
            String bucket) {
        assertThatThrownBy(() -> store.getBucketLifecycleConfiguration(
                GetBucketLifecycleConfigurationRequest.builder()
                        .bucket(bucket).build()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.putBucketLifecycleConfiguration(
                put(bucket)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.deleteBucketLifecycle(
                DeleteBucketLifecycleRequest.builder()
                        .bucket(bucket).build()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** Records the backend bucket each lifecycle operation reached. */
    private static final class LifecycleRecorder extends ForwardingBlobStore {
        private final List<String> buckets = new ArrayList<>();

        LifecycleRecorder() {
            super(TestUtils.createTransientBlobStore());
        }

        @Override
        public boolean supportsBucketLifecycle() {
            return true;
        }

        @Override
        public GetBucketLifecycleConfigurationResponse
                getBucketLifecycleConfiguration(
                        GetBucketLifecycleConfigurationRequest request) {
            buckets.add(request.bucket());
            return GetBucketLifecycleConfigurationResponse.builder().build();
        }

        @Override
        public PutBucketLifecycleConfigurationResponse
                putBucketLifecycleConfiguration(
                        PutBucketLifecycleConfigurationRequest request) {
            buckets.add(request.bucket());
            return PutBucketLifecycleConfigurationResponse.builder().build();
        }

        @Override
        public DeleteBucketLifecycleResponse deleteBucketLifecycle(
                DeleteBucketLifecycleRequest request) {
            buckets.add(request.bucket());
            return DeleteBucketLifecycleResponse.builder().build();
        }
    }
}
