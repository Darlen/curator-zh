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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

class CuratorCacheListenerBuilderImpl implements CuratorCacheListenerBuilder
{
    private final List<CuratorCacheListener> listeners = new ArrayList<>();
    private boolean afterInitializedOnly = false;
    private Predicate<String> pathFilter = __ -> true;

    @Override
    public CuratorCacheListenerBuilder forAll(CuratorCacheListener listener)
    {
        listeners.add(listener);
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder forCreates(Consumer<ChildData> listener)
    {
        listeners.add((type, oldNode, node) -> {
            if ( type == CuratorCacheListener.Type.NODE_CREATED )
            {
                listener.accept(node);
            }
        });
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder forChanges(ChangeListener listener)
    {
        listeners.add((type, oldNode, node) -> {
            if ( type == CuratorCacheListener.Type.NODE_CHANGED )
            {
                listener.event(oldNode, node);
            }
        });
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder forCreatesAndChanges(ChangeListener listener)
    {
        listeners.add((type, oldNode, node) -> {
            if ( (type == CuratorCacheListener.Type.NODE_CHANGED) || (type == CuratorCacheListener.Type.NODE_CREATED) )
            {
                listener.event(oldNode, node);
            }
        });
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder forDeletes(Consumer<ChildData> listener)
    {
        listeners.add((type, oldNode, node) -> {
            if ( type == CuratorCacheListener.Type.NODE_DELETED )
            {
                listener.accept(oldNode);
            }
        });
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder forInitialized(Runnable listener)
    {
        CuratorCacheListener localListener = new CuratorCacheListener()
        {
            @Override
            public void event(Type type, ChildData oldData, ChildData data)
            {
                // NOP
            }

            @Override
            public void initialized()
            {
                listener.run();
            }
        };
        listeners.add(localListener);
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder forPathChildrenCache(CuratorFramework client, PathChildrenCacheListener listener, String basePath)
    {
        pathFilter = p -> ZKPaths.getPathAndNode(p).getPath().equals(basePath);
        listeners.add(new PathChildrenCacheListenerWrapper(client, listener));
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder forTreeCache(CuratorFramework client, TreeCacheListener listener)
    {
        listeners.add(new TreeCacheListenerWrapper(client, listener));
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder forNodeCache(NodeCacheListener listener)
    {
        listeners.add(new NodeCacheListenerWrapper(listener));
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder afterInitialized()
    {
        afterInitializedOnly = true;
        return this;
    }

    @Override
    public CuratorCacheListenerBuilder withPathFilter(Predicate<String> pathFilter)
    {
        this.pathFilter = (pathFilter != null) ? (p -> (p == null) || pathFilter.test(p)) : (__ -> true);
        return this;
    }

    @Override
    public CuratorCacheListener build()
    {
        List<CuratorCacheListener> copy = new ArrayList<>(listeners);
        return new CuratorCacheListener()
        {
            private volatile boolean isInitialized = !afterInitializedOnly;

            @Override
            public void event(Type type, ChildData oldData, ChildData data)
            {
                if ( isInitialized )
                {
                    ChildData filterData = (data != null) ? data : oldData;
                    if ( pathFilter.test(filterData.getPath()) )
                    {
                        copy.forEach(l -> l.event(type, oldData, data));
                    }
                }
            }

            @Override
            public void initialized()
            {
                isInitialized = true;
                copy.forEach(CuratorCacheListener::initialized);
            }
        };
    }
}