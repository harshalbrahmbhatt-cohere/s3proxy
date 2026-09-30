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

package org.gaul.s3proxy.blobstore;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

/**
 * The default removeBlobs deletes key by key, and each of those deletes
 * carries the batch's bypass header, so a store that answers DeleteObject
 * alone still judges every key against the bypass the batch asked for.
 */
public final class RemoveBlobsBypassTest {
    @Test
    public void testDefaultRemoveBlobsCarriesBypassToEachKey()
            throws Exception {
        List<DeleteObjectRequest> deletes = new CopyOnWriteArrayList<>();
        // Only removeBlob is answered; every default runs as written.
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().equals("removeBlob") &&
                    args[0] instanceof DeleteObjectRequest request) {
                deletes.add(request);
                return DeleteObjectResponse.builder().build();
            }
            if (method.isDefault()) {
                return InvocationHandler.invokeDefault(proxy, method, args);
            }
            throw new UnsupportedOperationException(method.getName());
        };
        BlobStore blobStore = (BlobStore) Proxy.newProxyInstance(
                BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, handler);

        var result = blobStore.removeBlobs(DeleteObjectsRequest.builder()
                .bucket("bucket")
                .bypassGovernanceRetention(true)
                .delete(d -> d.objects(
                        ObjectIdentifier.builder().key("a").build(),
                        ObjectIdentifier.builder().key("b")
                                .versionId("v").build()))
                .build());

        assertThat(result.deleted()).hasSize(2);
        assertThat(deletes).hasSize(2)
                .allSatisfy(request -> assertThat(
                        request.bypassGovernanceRetention()).isTrue());
    }
}
