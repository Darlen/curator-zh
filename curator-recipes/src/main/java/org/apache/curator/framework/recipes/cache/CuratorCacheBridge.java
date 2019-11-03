/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.curator.framework.recipes.cache;

import org.apache.curator.framework.listen.Listenable;
import java.io.Closeable;
import java.util.stream.Stream;

/**
 * A facade that uses {@link org.apache.curator.framework.recipes.cache.CuratorCache} if
 * persistent watches are available or a {@link org.apache.curator.framework.recipes.cache.TreeCache}
 * otherwise
 */
@SuppressWarnings("deprecation")
public interface CuratorCacheBridge extends Closeable
{
    /**
     * Start the cache. This will cause a complete refresh from the cache's root node and generate
     * events for all nodes found, etc.
     */
    void start();

    /**
     * Close the cache, stop responding to events, etc.
     */
    @Override
    void close();

    /**
     * Return the listener container so that listeners can be registered to be notified of changes to the cache
     *
     * @return listener container
     */
    Listenable<CuratorCacheListener> listenable();

    /**
     * Return a stream over the storage entries that are the immediate children of the given node.
     *
     * @param fromParent the parent node - determines the children returned in the stream
     * @return stream over entries
     */
    Stream<ChildData> streamImmediateChildren(String fromParent);
}
