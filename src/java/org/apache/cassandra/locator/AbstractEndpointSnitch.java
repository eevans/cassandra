/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;

public abstract class AbstractEndpointSnitch implements IEndpointSnitch, IEndpointStateChangeSubscriber
{
    public abstract int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2);

    /**
     * Current view of the topology. These are immutable and are updated by clone-update-swap, which allows
     * us to return them direct to a client without worrying about cost of copying or synchronisation.
     * We can't use CSLM here because when calculating data placement we can't afford weakly consistent iteration.
     */
    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected Map<String, Set<InetAddress>> dcEndpoints = new HashMap<String, Set<InetAddress>>();
    protected Map<String, Set<String>> dcRacks = new HashMap<String, Set<String>>();
    /**
     * Sorts the <tt>Collection</tt> of node addresses by proximity to the given address
     * @param address the address to sort by proximity to
     * @param unsortedAddress the nodes to sort
     * @return a new sorted <tt>List</tt>
     */
    public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> unsortedAddress)
    {
        List<InetAddress> preferred = new ArrayList<InetAddress>(unsortedAddress);
        sortByProximity(address, preferred);
        return preferred;
    }

    /**
     * Sorts the <tt>List</tt> of node addresses, in-place, by proximity to the given address
     * @param address the address to sort the proximity by
     * @param addresses the nodes to sort
     */
    public void sortByProximity(final InetAddress address, List<InetAddress> addresses)
    {
        Collections.sort(addresses, new Comparator<InetAddress>()
        {
            public int compare(InetAddress a1, InetAddress a2)
            {
                return compareEndpoints(address, a1, a2);
            }
        });
    }

    public void gossiperStarting()
    {
        Gossiper.instance.register(this);
    }

    /**
     * update our knowledge of which racks a dc contains
     * TODO: this could be more efficient if we also stored rack -> endpoint mapping
     */
    protected void updateDcRacks()
    {
        Map<String, Set<String>> newDcRacks = new HashMap<String, Set<String>>();
        for (Map.Entry<String, Set<InetAddress>> dc : dcEndpoints.entrySet())
        {
            if (!newDcRacks.containsKey(dc.getKey()))
                newDcRacks.put(dc.getKey(), new HashSet<String>());
            for (InetAddress ep : dc.getValue())
                newDcRacks.get(dc.getKey()).add(getRack(ep));
        }
        dcRacks = newDcRacks;
    }

    protected Map<String, Set<InetAddress>> deepCopyEndpointsMap()
    {
        Map<String, Set<InetAddress>> copy = new HashMap<String, Set<InetAddress>>(dcEndpoints.size());
        for (Map.Entry<String, Set<InetAddress>> entry : dcEndpoints.entrySet())
            copy.put(entry.getKey(), new HashSet<InetAddress>(entry.getValue()));
        return copy;
    }

    protected void addEndpoint(InetAddress ep)
    {
        lock.writeLock().lock();
        try
        {
            String dc = getDatacenter(ep);
            if (dcEndpoints.containsKey(dc) && dcEndpoints.get(dc).contains(ep))
                return;

            Map<String, Set<InetAddress>> newEndpoints = deepCopyEndpointsMap();
            if (!newEndpoints.containsKey(dc))
                newEndpoints.put(dc, new HashSet<InetAddress>());
            newEndpoints.get(dc).add(ep);
            dcEndpoints = newEndpoints;
            updateDcRacks();
        } finally
        {
            lock.writeLock().unlock();
        }
    }

    protected void removeEndpoint(InetAddress ep)
    {
        lock.writeLock().lock();
        try
        {
            String dc = getDatacenter(ep);
            if (!dcEndpoints.containsKey(dc) || !dcEndpoints.get(dc).contains(ep))
                return;

            Map<String, Set<InetAddress>> newEndpoints = deepCopyEndpointsMap();
            newEndpoints.get(dc).remove(ep);
            if (newEndpoints.get(dc).isEmpty())
                newEndpoints.remove(dc);
            dcEndpoints = newEndpoints;
            updateDcRacks();
        } finally
        {
            lock.writeLock().unlock();
        }
    }

    @Override
    /**
     * This ensures we get a consistent view of endpoint and racks
     */
    public Topology getTopology()
    {
        lock.readLock().lock();
        try
        {
            final Map<String, Set<InetAddress>> epSnapshot = dcEndpoints;
            final Map<String, Set<String>> racksSnapshot = dcRacks;
            return new Topology()
            {
                @Override
                public Map<String, Set<InetAddress>> getDatacenterEndpoints()
                {
                    return Collections.unmodifiableMap(epSnapshot);
                }
    
                @Override
                public Map<String, Set<String>> getDatacenterRacks()
                {
                    return Collections.unmodifiableMap(racksSnapshot);
                }
            };
        } finally
        {
            lock.readLock().unlock();
        }
    }

    private void checkState(InetAddress endpoint, VersionedValue value)
    {
        if (value == null)
            return;
        String[] pieces = value.value.split(VersionedValue.DELIMITER_STR);
        if (VersionedValue.STATUS_NORMAL.equals(pieces[0]))
            addEndpoint(endpoint);
        else
            removeEndpoint(endpoint);
    }

    @Override
    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        checkState(endpoint, state.getApplicationState(ApplicationState.STATUS));
    }

    @Override
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state == ApplicationState.STATUS)
            checkState(endpoint, value);
    }

    @Override
    public void onDead(InetAddress endpoint, EndpointState state)
    {
        checkState(endpoint, state.getApplicationState(ApplicationState.STATUS));
    }

    @Override
    public void onJoin(InetAddress endpoint, EndpointState state)
    {
        checkState(endpoint, state.getApplicationState(ApplicationState.STATUS));
    }

    @Override
    public void onRemove(InetAddress endpoint)
    {
        removeEndpoint(endpoint);
    }

    @Override
    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        checkState(endpoint, state.getApplicationState(ApplicationState.STATUS));
    }
}
