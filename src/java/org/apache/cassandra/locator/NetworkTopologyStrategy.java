/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

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
@SuppressWarnings("unchecked")
public class NetworkTopologyStrategy extends AbstractReplicationStrategy
{
    private static final int DF_ALL = 0;

    private final IEndpointSnitch snitch;
    private final Map<String, Integer> datacenterReplication;
    private final Map<String, Integer> datacenterDistribution;
    private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);

    public NetworkTopologyStrategy(String table, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException
    {
        super(table, tokenMetadata, snitch, configOptions);
        this.snitch = snitch;

        Map<String, Integer> dcReplication = new HashMap<String, Integer>();
        Map<String, Integer> dcDistribution = new HashMap<String, Integer>();
        if (configOptions != null)
        {
            for (Entry<String, String> entry : configOptions.entrySet())
            {
                String dc = entry.getKey();
                if (dc.equalsIgnoreCase("replication_factor"))
                    throw new ConfigurationException("replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
                String[] values = entry.getValue().split("/");
                dcReplication.put(dc, Integer.valueOf(values[0]));
                if (values.length > 1)
                    dcDistribution.put(dc, Integer.valueOf(values[1]));
                else
                    dcDistribution.put(dc, DF_ALL);
            }
        }

        datacenterReplication = Collections.unmodifiableMap(dcReplication);
        datacenterDistribution = Collections.unmodifiableMap(dcDistribution);
        logger.debug("Configured datacenter replicas are {}", FBUtilities.toString(datacenterReplication));
    }

    /**
     * By successively applying filters to the token iterator we can avoid TokenMetadata.updateNormalTokens
     */
    private interface TokenIteratorFactory
    {
        Iterator<Token> create(Token startToken);
    }

    private TokenIteratorFactory defaultTokenIterator(final TokenMetadata metadata)
    {
        return new TokenIteratorFactory()
        {
            @Override
            public Iterator<Token> create(Token startToken)
            {
                return TokenMetadata.ringIterator(metadata.sortedTokens(), startToken, false);
            }
        };
    }

    private TokenIteratorFactory filteredByDatacenter(final TokenIteratorFactory fact, final TokenMetadata metadata, final String dc)
    {
        return new TokenIteratorFactory()
        {
            @Override
            public Iterator<Token> create(Token startToken)
            {
                return Iterators.filter(fact.create(startToken), new Predicate<Token>()
                {
                    @Override
                    public boolean apply(Token token)
                    {
                        return snitch.getDatacenter(metadata.getEndpoint(token)).equals(dc);
                    }
                });
            }
        };
    }

    private TokenIteratorFactory filteredByDistributionSet(final TokenIteratorFactory fact, final TokenMetadata metadata, final InetAddress endpoint)
    {
        int df = datacenterDistribution.get(snitch.getDatacenter(endpoint));
        if (df == DF_ALL)
            return fact;

        // we calculate the distribution set for endpoint by taking the first (lowest) token belonging to
        // endpoint and move around the ring collecting the next DF endpoints
        // TODO: we could amortize O(NlogN) operations (adding tokens to TreeSet) if we memoize the distSet for each endpoint
        Set<Token> tokens = new TreeSet<Token>(metadata.getTokens(endpoint));
        Token firstToken = tokens.iterator().next();
        final Set<InetAddress> distSet = nextDistinctEndpoints(metadata, fact, firstToken, df);

        if (logger.isDebugEnabled())
            logger.debug("replicas for endpoint {} restricted to the set {}",
                         new Object[] { endpoint, StringUtils.join(distSet, ",")});

        return new TokenIteratorFactory()
        {
            @Override
            public Iterator<Token> create(Token startToken)
            {
                return Iterators.filter(fact.create(startToken), new Predicate<Token>()
                {
                    @Override
                    public boolean apply(Token token)
                    {
                        return distSet.contains(metadata.getEndpoint(token));
                    }
                });
            }
        };
    }

    @Override
    public List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata)
    {
        List<InetAddress> endpoints = new ArrayList<InetAddress>(getReplicationFactor());

        for (Entry<String, Integer> dcEntry : datacenterReplication.entrySet())
        {
            String dcName = dcEntry.getKey();
            int dcReplicas = dcEntry.getValue();

            TokenIteratorFactory it = defaultTokenIterator(tokenMetadata);
            TokenIteratorFactory dcTokens = filteredByDatacenter(it, tokenMetadata, dcName);

            // get the primary endpoint for this token in this DC
            Iterator<Token> dcIterator = dcTokens.create(searchToken);
            if (!dcIterator.hasNext())
                continue;
            InetAddress primaryEndpoint = tokenMetadata.getEndpoint(dcIterator.next());

            TokenIteratorFactory distSetTokens = filteredByDistributionSet(dcTokens, tokenMetadata, primaryEndpoint);

            Set<InetAddress> dcEndpoints = nextDistinctEndpoints(tokenMetadata, distSetTokens, searchToken, dcReplicas);

            if (logger.isDebugEnabled())
                logger.debug("{} endpoints in datacenter {} for token {} ",
                             new Object[] { StringUtils.join(dcEndpoints, ","), dcName, searchToken});
            endpoints.addAll(dcEndpoints);
        }

        return endpoints;
    }

    private Set<InetAddress> nextDistinctEndpoints(TokenMetadata metadata, TokenIteratorFactory fact, Token startToken, int total)
    {
        HashSet<InetAddress> endpoints = new HashSet<InetAddress>(total);
        // first pass: only collect replicas on unique racks
        nextEndpoints(metadata, fact.create(startToken), total, true, endpoints);
        // second pass: if replica count has not been achieved from unique racks, add nodes from duplicate racks
        if (endpoints.size() < total)
            nextEndpoints(metadata, fact.create(startToken), total, false, endpoints);
        return endpoints;
    }

    private void nextEndpoints(TokenMetadata metadata, Iterator<Token> iter, int total, boolean distinctRack, Set<InetAddress> endpoints)
    {
        Set<String> racks = new HashSet<String>();
        while (endpoints.size() < total && iter.hasNext())
        {
            Token token = iter.next();
            InetAddress endpoint = metadata.getEndpoint(token);
            String rack = snitch.getRack(endpoint);
            if (!distinctRack || racks.add(rack))
                endpoints.add(endpoint);
        }
    }

    @Override
    public int getReplicationFactor()
    {
        int total = 0;
        for (int repFactor : datacenterReplication.values())
            total += repFactor;
        return total;
    }

    public int getReplicationFactor(String dc)
    {
        return datacenterReplication.get(dc);
    }

    public Set<String> getDatacenters()
    {
        return datacenterReplication.keySet();
    }

    @Override
    public void validateOptions() throws ConfigurationException
    {
        for (Entry<String, String> e : this.configOptions.entrySet())
        {
            String[] values = e.getValue().split("/");
            if (values.length < 1 || values.length > 2)
                throw new ConfigurationException("Replication factor must be in the format \"rf/df\"");
            validateReplicationFactor(values[0]);
            if (values.length > 1)
            {
                validateReplicationFactor(values[1]);
                if (Integer.parseInt(values[1]) < Integer.parseInt(values[0]))
                    throw new ConfigurationException("Distribution factor must be greater than or equal to Replication factor.");
            }
        }

    }
}
