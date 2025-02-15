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

package org.apache.curator.framework.recipes.leader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.AfterConnectionEstablished;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.utils.PathUtils;

/**
 * <p>
 * Abstraction to select a "leader" amongst multiple contenders in a group of JMVs connected to
 * a Zookeeper cluster. If a group of N thread/processes contend for leadership one will
 * randomly be assigned leader until it releases leadership at which time another one from the
 * group will randomly be chosen
 * </p>
 */
public class LeaderLatch implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    private final String latchPath;
    private final String id;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final AtomicBoolean hasLeadership = new AtomicBoolean(false);
    private final AtomicReference<String> ourPath = new AtomicReference<String>();
    private final ListenerContainer<LeaderLatchListener> listeners = new ListenerContainer<LeaderLatchListener>();
    private final CloseMode closeMode;
    private final AtomicReference<Future<?>> startTask = new AtomicReference<Future<?>>();

    private final ConnectionStateListener listener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };

    private static final String LOCK_NAME = "latch-";

    private static final LockInternalsSorter sorter = new LockInternalsSorter()
    {
        @Override
        public String fixForSorting(String str, String lockName)
        {
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

    public enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * How to handle listeners when the latch is closed
     */
    public enum CloseMode
    {
        /**
         * When the latch is closed, listeners will *not* be notified (default behavior)
         */
        SILENT,

        /**
         * When the latch is closed, listeners *will* be notified
         */
        NOTIFY_LEADER
    }

    /**
     * @param client    the client
     * @param latchPath the path for this leadership group
     */
    public LeaderLatch(CuratorFramework client, String latchPath)
    {
        this(client, latchPath, "", CloseMode.SILENT);
    }

    /**
     * @param client    the client
     * @param latchPath the path for this leadership group
     * @param id        participant ID
     */
    public LeaderLatch(CuratorFramework client, String latchPath, String id)
    {
        this(client, latchPath, id, CloseMode.SILENT);
    }

    /**
     * @param client    the client
     * @param latchPath the path for this leadership group Leader选举根节点路径
     * @param id        participant ID 客户端id，用来标记客户端，即客户端编号、名称
     * @param closeMode behaviour of listener on explicit close.
     *                  SILENT-关闭时不触发监听器回调，NOTIFY_LEADER-关闭时触发监听器回调方法，默认不触发
     */
    public LeaderLatch(CuratorFramework client, String latchPath, String id, CloseMode closeMode)
    {
        this.client = Preconditions.checkNotNull(client, "client cannot be null").newWatcherRemoveCuratorFramework();
        this.latchPath = PathUtils.validatePath(latchPath);
        this.id = Preconditions.checkNotNull(id, "id cannot be null");
        this.closeMode = Preconditions.checkNotNull(closeMode, "closeMode cannot be null");
    }

    /**
     * Add this instance to the leadership election and attempt to acquire leadership.
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        startTask.set(AfterConnectionEstablished.execute(client, new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    internalStart();
                }
                finally
                {
                    startTask.set(null);
                }
            }
        }));
    }

    /**
     * Remove this instance from the leadership election. If this instance is the leader, leadership
     * is released. IMPORTANT: the only way to release leadership is by calling close(). All LeaderLatch
     * instances must eventually be closed.
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        close(closeMode);
    }

    /**
     * Remove this instance from the leadership election. If this instance is the leader, leadership
     * is released. IMPORTANT: the only way to release leadership is by calling close(). All LeaderLatch
     * instances must eventually be closed.
     *
     * @param closeMode allows the default close mode to be overridden at the time the latch is closed.
     * @throws IOException errors
     */
    public synchronized void close(CloseMode closeMode) throws IOException
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED), "Already closed or has not been started");
        Preconditions.checkNotNull(closeMode, "closeMode cannot be null");

        cancelStartTask();

        try
        {
            setNode(null);
            client.removeWatchers();
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new IOException(e);
        }
        finally
        {
            client.getConnectionStateListenable().removeListener(listener);

            switch ( closeMode )
            {
            case NOTIFY_LEADER:
            {
                setLeadership(false);
                listeners.clear();
                break;
            }

            default:
            {
                listeners.clear();
                setLeadership(false);
                break;
            }
            }
        }
    }

    @VisibleForTesting
    protected boolean cancelStartTask()
    {
        Future<?> localStartTask = startTask.getAndSet(null);
        if ( localStartTask != null )
        {
            localStartTask.cancel(true);
            return true;
        }
        return false;
    }

    /**
     * Attaches a listener to this LeaderLatch
     * <p>
     * Attaching the same listener multiple times is a noop from the second time on.
     * </p><p>
     * All methods for the listener are run using the provided Executor.  It is common to pass in a single-threaded
     * executor so that you can be certain that listener methods are called in sequence, but if you are fine with
     * them being called out of order you are welcome to use multiple threads.
     * </p>
     *
     * @param listener the listener to attach
     */
    public void addListener(LeaderLatchListener listener)
    {
        listeners.addListener(listener);
    }

    /**
     * Attaches a listener to this LeaderLatch
     * <p>
     * Attaching the same listener multiple times is a noop from the second time on.
     * </p><p>
     * All methods for the listener are run using the provided Executor.  It is common to pass in a single-threaded
     * executor so that you can be certain that listener methods are called in sequence, but if you are fine with
     * them being called out of order you are welcome to use multiple threads.
     * </p>
     *
     * @param listener the listener to attach
     * @param executor An executor to run the methods for the listener on.
     */
    public void addListener(LeaderLatchListener listener, Executor executor)
    {
        listeners.addListener(listener, executor);
    }

    /**
     * Removes a given listener from this LeaderLatch
     *
     * @param listener the listener to remove
     */
    public void removeListener(LeaderLatchListener listener)
    {
        listeners.removeListener(listener);
    }

    /**
     * <p>Causes the current thread to wait until this instance acquires leadership
     * unless the thread is {@linkplain Thread#interrupt interrupted} or {@linkplain #close() closed}.</p>
     * <p>If this instance already is the leader then this method returns immediately.</p>
     * <p></p>
     * <p>Otherwise the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of three things happen:</p>
     * <ul>
     * <li>This instance becomes the leader</li>
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread</li>
     * <li>The instance is {@linkplain #close() closed}</li>
     * </ul>
     * <p>If the current thread:</p>
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * <p>then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.</p>
     *
     * @throws InterruptedException if the current thread is interrupted
     *                              while waiting
     * @throws EOFException         if the instance is {@linkplain #close() closed}
     *                              while waiting
     */
    public void await() throws InterruptedException, EOFException
    {
        synchronized(this)
        {
            while ( (state.get() == State.STARTED) && !hasLeadership.get() )
            {
                wait();
            }
        }
        if ( state.get() != State.STARTED )
        {
            throw new EOFException();
        }
    }

    /**
     * <p>Causes the current thread to wait until this instance acquires leadership
     * unless the thread is {@linkplain Thread#interrupt interrupted},
     * the specified waiting time elapses or the instance is {@linkplain #close() closed}.</p>
     * <p></p>
     * <p>If this instance already is the leader then this method returns immediately
     * with the value {@code true}.</p>
     * <p></p>
     * <p>Otherwise the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of four things happen:</p>
     * <ul>
     * <li>This instance becomes the leader</li>
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread</li>
     * <li>The specified waiting time elapses.</li>
     * <li>The instance is {@linkplain #close() closed}</li>
     * </ul>
     * <p></p>
     * <p>If the current thread:</p>
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * <p>then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.</p>
     * <p></p>
     * <p>If the specified waiting time elapses or the instance is {@linkplain #close() closed}
     * then the value {@code false} is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.</p>
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if the count reached zero and {@code false}
     * if the waiting time elapsed before the count reached zero or the instances was closed
     * @throws InterruptedException if the current thread is interrupted
     *                              while waiting
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException
    {
        long waitNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);

        synchronized(this)
        {
            while ( true )
            {
                if ( state.get() != State.STARTED )
                {
                    return false;
                }

                if ( hasLeadership() )
                {
                    return true;
                }

                if ( waitNanos <= 0 )
                {
                    return false;
                }

                long startNanos = System.nanoTime();
                TimeUnit.NANOSECONDS.timedWait(this, waitNanos);
                long elapsed = System.nanoTime() - startNanos;
                waitNanos -= elapsed;
            }
        }
    }

    /**
     * Return this instance's participant Id
     *
     * @return participant Id
     */
    public String getId()
    {
        return id;
    }

    /**
     * Returns this instances current state, this is the only way to verify that the object has been closed before
     * closing again.  If you try to close a latch multiple times, the close() method will throw an
     * IllegalArgumentException which is often not caught and ignored (CloseableUtils.closeQuietly() only looks for
     * IOException).
     *
     * @return the state of the current instance
     */
    public State getState()
    {
        return state.get();
    }

    /**
     * <p>
     * Returns the set of current participants in the leader selection
     * </p>
     * <p>
     * <p>
     * <B>NOTE</B> - this method polls the ZK server. Therefore it can possibly
     * return a value that does not match {@link #hasLeadership()} as hasLeadership
     * uses a local field of the class.
     * </p>
     *
     * @return participants
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Participant> getParticipants() throws Exception
    {
        Collection<String> participantNodes = LockInternals.getParticipantNodes(client, latchPath, LOCK_NAME, sorter);
        return LeaderSelector.getParticipants(client, participantNodes);
    }

    /**
     * <p>
     * Return the id for the current leader. If for some reason there is no
     * current leader, a dummy participant is returned.
     * </p>
     * <p>
     * <p>
     * <B>NOTE</B> - this method polls the ZK server. Therefore it can possibly
     * return a value that does not match {@link #hasLeadership()} as hasLeadership
     * uses a local field of the class.
     * </p>
     *
     * @return leader
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Participant getLeader() throws Exception
    {
        Collection<String> participantNodes = LockInternals.getParticipantNodes(client, latchPath, LOCK_NAME, sorter);
        return LeaderSelector.getLeader(client, participantNodes);
    }

    /**
     * Return true if leadership is currently held by this instance
     *
     * @return true/false
     */
    public boolean hasLeadership()
    {
        return (state.get() == State.STARTED) && hasLeadership.get();
    }

    /**
     * Return this instance's lock node path. IMPORTANT: this instance
     * owns the path returned. This method is meant for reference only. Also,
     * it is possible for <code>null</code> to be returned. The path, if any,
     * returned is not guaranteed to be valid at any point in the future as internal
     * state changes might require the instance to delete and create a new path.
     *
     * @return lock node path or <code>null</code>
     */
    public String getOurPath()
    {
        return ourPath.get();
    }

    @VisibleForTesting
    volatile CountDownLatch debugResetWaitLatch = null;

    @VisibleForTesting
    void reset() throws Exception
    {
        //设置当前没成为到leader
        setLeadership(false);
        setNode(null);

        BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( debugResetWaitLatch != null )
                {
                    debugResetWaitLatch.await();
                    debugResetWaitLatch = null;
                }

                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    setNode(event.getName());
                    if ( state.get() == State.CLOSED )
                    {
                        setNode(null);
                    }
                    else
                    {
                        //为latchPath下每个children设置监听事件
                        getChildren();
                    }
                }
                else
                {
                    log.error("getChildren() failed. rc = " + event.getResultCode());
                }
            }
        };
        //在latchPath下创建临时有序节点，节点内容为serverId，并设置异步回调
        //注意：创建完临时有序节点后，会触发到回调BackgroundCallback里的getChildren()方法
        client.create().creatingParentContainersIfNeeded().withProtection()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL).inBackground(callback)
                .forPath(ZKPaths.makePath(latchPath, LOCK_NAME), LeaderSelector.getIdBytes(id));
    }

    private synchronized void internalStart()
    {
        if ( state.get() == State.STARTED )
        {
            //为zk添加连接监听器，连接器监听到重连时间后也会调用reset()方法
            client.getConnectionStateListenable().addListener(listener);
            try
            {
                //初始化事件
                reset();
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                log.error("An error occurred checking resetting leadership.", e);
            }
        }
    }

    @VisibleForTesting
    volatile CountDownLatch debugCheckLeaderShipLatch = null;

    /**
     * 核心流程就是
     *
     * 1. zk客户端往同一路径下创建临时节点，创建后回调callBack
     * 2. 在回调事件中判断自身节点是否是节点编号最小的一个
     * 3. 如果是，则抢主成功，如果不是，设置对前一个节点(编号更小的)的删除事件的监听器，删除事件触发后重新进行抢主
     * @param children
     * @throws Exception
     */
    private void checkLeadership(List<String> children) throws Exception
    {
        if ( debugCheckLeaderShipLatch != null )
        {
            debugCheckLeaderShipLatch.await();
        }

        final String localOurPath = ourPath.get();
        //按节点编号排序
        List<String> sortedChildren = LockInternals.getSortedChildren(LOCK_NAME, sorter, children);
        int ourIndex = (localOurPath != null) ? sortedChildren.indexOf(ZKPaths.getNodeFromPath(localOurPath)) : -1;
        if ( ourIndex < 0 )
        {
            log.error("Can't find our node. Resetting. Index: " + ourIndex);
            reset();
        }
        else if ( ourIndex == 0 )
        {
            //如果当前节点编号最小 即抢主成功 设当前节点为leader
            setLeadership(true);
        }
        else
        {
            //抢主失败 监听前面一个(节点编号更小的)节点
            String watchPath = sortedChildren.get(ourIndex - 1);
            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    //监听前一个节点的删除事件，重新进入getChildren方法判断是否抢主成功
                    if ( (state.get() == State.STARTED) && (event.getType() == Event.EventType.NodeDeleted) && (localOurPath != null) )
                    {
                        try
                        {
                            getChildren();
                        }
                        catch ( Exception ex )
                        {
                            ThreadUtils.checkInterrupted(ex);
                            log.error("An error occurred checking the leadership.", ex);
                        }
                    }
                }
            };

            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                    {
                        // previous node is gone - reset
                        reset();
                    }
                }
            };
            //设置对前一个节点删除时间的监听器，并在异步回调里重新进行抢主
            // use getData() instead of exists() to avoid leaving unneeded watchers which is a type of resource leak
            client.getData().usingWatcher(watcher).inBackground(callback).forPath(ZKPaths.makePath(latchPath, watchPath));
        }
    }

    /**
     * 创建完临时有序节点后，会触发到回调BackgroundCallback里的getChildren()方法
     * @throws Exception
     */

    private void getChildren() throws Exception
    {
        BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    //最终在获取到latchPath下子节点信息后，进入checkLeadership()方法
                    checkLeadership(event.getChildren());
                }
            }
        };
        //获取latchPath下子节点信息，获取成功后触发异步回调callback
        client.getChildren().inBackground(callback).forPath(ZKPaths.makePath(latchPath, null));
    }

    @VisibleForTesting
    protected void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
            default:
            {
                // NOP
                break;
            }

            case RECONNECTED:
            {
                try
                {
                    if ( client.getConnectionStateErrorPolicy().isErrorState(ConnectionState.SUSPENDED) || !hasLeadership.get() )
                    {
                        reset();
                    }
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    log.error("Could not reset leader latch", e);
                    setLeadership(false);
                }
                break;
            }

            case SUSPENDED:
            {
                if ( client.getConnectionStateErrorPolicy().isErrorState(ConnectionState.SUSPENDED) )
                {
                    setLeadership(false);
                }
                break;
            }

            case LOST:
            {
                setLeadership(false);
                break;
            }
        }
    }

    private synchronized void setLeadership(boolean newValue)
    {
        boolean oldValue = hasLeadership.getAndSet(newValue);

        if ( oldValue && !newValue )
        { // Lost leadership, was true, now false
            listeners.forEach(new Function<LeaderLatchListener, Void>()
                {
                    @Override
                    public Void apply(LeaderLatchListener listener)
                    {
                        listener.notLeader();
                        return null;
                    }
                });
        }
        else if ( !oldValue && newValue )
        { // Gained leadership, was false, now true
            listeners.forEach(new Function<LeaderLatchListener, Void>()
                {
                    @Override
                    public Void apply(LeaderLatchListener input)
                    {
                        input.isLeader();
                        return null;
                    }
                });
        }

        notifyAll();
    }

    private void setNode(String newValue) throws Exception
    {
        String oldPath = ourPath.getAndSet(newValue);
        if ( oldPath != null )
        {
            client.delete().guaranteed().inBackground().forPath(oldPath);
        }
    }
}
