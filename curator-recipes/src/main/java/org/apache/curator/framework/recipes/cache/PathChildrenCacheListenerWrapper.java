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

class PathChildrenCacheListenerWrapper implements CuratorCacheListener
{
    private final PathChildrenCacheListener listener;
    private final CuratorFramework client;

    PathChildrenCacheListenerWrapper(CuratorFramework client, PathChildrenCacheListener listener)
    {
        this.listener = listener;
        this.client = client;
    }

    @Override
    public void event(Type type, ChildData oldData, ChildData data)
    {
        switch ( type )
        {
            case NODE_CREATED:
            {
                sendEvent(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_ADDED, data));
                break;
            }

            case NODE_CHANGED:
            {
                sendEvent(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data));
                break;
            }

            case NODE_DELETED:
            {
                sendEvent(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, oldData));
                break;
            }
        }
    }

    @Override
    public void initialized()
    {
        sendEvent(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.INITIALIZED, null));
    }

    private void sendEvent(PathChildrenCacheEvent event)
    {
        try
        {
            listener.childEvent(client, event);
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }
}