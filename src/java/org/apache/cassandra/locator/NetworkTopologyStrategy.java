/*
 *
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
 *
 */

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.IEndpointSnitch.Topology;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This Replication Strategy takes a property file that gives the intended
 * replication factor in each datacenter.  The sum total of the datacenter
 * replication factor values should be equal to the keyspace replication
 * factor.
 * <p/>
 * So for example, if the keyspace replication factor is 6, the
 * datacenter replication factors could be 3, 2, and 1 - so 3 replicas in
 * one datacenter, 2 in another, and 1 in another - totalling 6.
 * <p/>
 * This class also caches the Endpoints and invalidates the cache if there is a
 * change in the number of tokens.
 */
public class NetworkTopologyStrategy extends AbstractReplicationStrategy
{
    private final IEndpointSnitch snitch;
    private final Map<String, Integer> datacenters;
    private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);

    public NetworkTopologyStrategy(String table, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException
    {
        super(table, tokenMetadata, snitch, configOptions);
        this.snitch = snitch;

        Map<String, Integer> newDatacenters = new HashMap<String, Integer>();
        if (configOptions != null)
        {
            for (Entry<String, String> entry : configOptions.entrySet())
            {
                String dc = entry.getKey();
                if (dc.equalsIgnoreCase("replication_factor"))
                    throw new ConfigurationException("replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
                Integer replicas = Integer.valueOf(entry.getValue());
                newDatacenters.put(dc, replicas);
            }
        }

        datacenters = Collections.unmodifiableMap(newDatacenters);
        logger.debug("Configured datacenter replicas are {}", FBUtilities.toString(datacenters));
    }

    /**
     * calculate endpoints in one pass through the tokens by tracking our progress in each DC, rack etc.
     */
    @SuppressWarnings("serial")
    @Override
    public List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata)
    {
        Set<InetAddress> replicas = new HashSet<InetAddress>();
        // replicas we have found in each DC
        Map<String, Set<InetAddress>> replicaEndpoints = new HashMap<String, Set<InetAddress>>(datacenters.size())
        {{
            for (Map.Entry<String, Integer> dc : datacenters.entrySet())
                put(dc.getKey(), new HashSet<InetAddress>(dc.getValue()));
        }};
        Topology topology = snitch.getTopology();
        // all endpoints in each DC, so we can check when we have exhausted all the members of a DC
        Map<String, Set<InetAddress>> allEndpoints = topology.getDatacenterEndpoints();
        // all racks in a DC so we can check when we have exhausted all racks in a DC
        Map<String, Set<String>> racks = topology.getDatacenterRacks();
        assert !allEndpoints.isEmpty() && !racks.isEmpty() : "snitch is not aware of any cluster members";

        // tracks the racks we have already placed replicas in
        Map<String, Set<String>> seenRacks = new HashMap<String, Set<String>>(datacenters.size())
        {{
            for (Map.Entry<String, Integer> dc : datacenters.entrySet())
                put(dc.getKey(), new HashSet<String>());
        }};
        // tracks the endpoints that we skipped over while looking for unique racks
        // when we relax the rack uniqueness we can append this to the current result so we don't have to wind back the iterator
        Map<String, Set<InetAddress>> skippedDcEndpoints = new HashMap<String, Set<InetAddress>>(datacenters.size())
        {{
            for (Map.Entry<String, Integer> dc : datacenters.entrySet())
                put(dc.getKey(), new LinkedHashSet<InetAddress>());
        }};
        Iterator<Token> tokenIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), searchToken, false);
        while (tokenIter.hasNext() && !replicaEndpoints.isEmpty())
        {
            Token next = tokenIter.next();
            InetAddress ep = tokenMetadata.getEndpoint(next);
            String dc = snitch.getDatacenter(ep);
            // have we already found all replicas for this dc?
            if (!replicaEndpoints.containsKey(dc) || !datacenters.containsKey(dc))
                continue;
            // can we skip checking the rack?
            if (seenRacks.get(dc).size() == racks.get(dc).size())
            {
                replicaEndpoints.get(dc).add(ep);
                replicas.add(ep);
            } else
            {
                String rack = snitch.getRack(ep);
                // is this a new rack?
                if (seenRacks.get(dc).contains(rack))
                {
                    skippedDcEndpoints.get(dc).add(ep);
                } else
                {
                    replicaEndpoints.get(dc).add(ep);
                    replicas.add(ep);
                    seenRacks.get(dc).add(rack);
                    // if we've run out of distinct racks, add the hosts we skipped past already (up to RF)
                    if (seenRacks.get(dc).size() == racks.get(dc).size() && !skippedDcEndpoints.get(dc).isEmpty())
                    {
                        Iterator<InetAddress> skippedIt = skippedDcEndpoints.get(dc).iterator();
                        while (skippedIt.hasNext() && replicaEndpoints.get(dc).size() < Math.min(allEndpoints.get(dc).size(), getReplicationFactor(dc)))
                        {
                            InetAddress nextSkipped = skippedIt.next();
                            replicaEndpoints.get(dc).add(nextSkipped);
                            replicas.add(nextSkipped);
                        }
                    }
                }
            }
            // check if we found RF replicas for this DC or whether we exhausted its endpoints
            if (replicaEndpoints.get(dc).size() >= Math.min(allEndpoints.get(dc).size(), getReplicationFactor(dc)))
            {
                if (logger.isDebugEnabled())
                    logger.debug("{} endpoints in datacenter {} for token {} ",
                            new Object[] { StringUtils.join(replicaEndpoints.get(dc), ","), dc, searchToken});
                replicaEndpoints.remove(dc);
            }
        }

        return new ArrayList<InetAddress>(replicas);
    }

    public int getReplicationFactor()
    {
        int total = 0;
        for (int repFactor : datacenters.values())
            total += repFactor;
        return total;
    }

    public int getReplicationFactor(String dc)
    {
        return datacenters.get(dc);
    }

    public Set<String> getDatacenters()
    {
        return datacenters.keySet();
    }

    public void validateOptions() throws ConfigurationException
    {
        for (Entry<String, String> e : this.configOptions.entrySet())
        {
            validateReplicationFactor(e.getValue());
        }

    }
}
