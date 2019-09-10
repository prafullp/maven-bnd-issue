/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 *
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package com.yahoo.ycsb.db;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

/**
 * Cassandra 2.x CQL client.
 *
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  // private static Logger logger =
  // LoggerFactory.getLogger(CassandraCQLClient.class);

  private CqlSession session = null;

  private static ConcurrentMap<Set<String>, PreparedStatement> readStmts = new ConcurrentHashMap<>();
  private static ConcurrentMap<Set<String>, PreparedStatement> scanStmts = new ConcurrentHashMap<>();
  private static ConcurrentMap<Set<String>, PreparedStatement> insertStmts = new ConcurrentHashMap<>();
  private static ConcurrentMap<Set<String>, PreparedStatement> updateStmts = new ConcurrentHashMap<>();
  private static AtomicReference<PreparedStatement> readAllStmt = new AtomicReference<>();
  private static AtomicReference<PreparedStatement> scanAllStmt = new AtomicReference<>();
  private static AtomicReference<PreparedStatement> deleteStmt = new AtomicReference<>();

  private static DefaultConsistencyLevel readConsistencyLevel = DefaultConsistencyLevel.LOCAL_QUORUM;
  private static DefaultConsistencyLevel writeConsistencyLevel = DefaultConsistencyLevel.LOCAL_QUORUM;

  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String HOSTS_PROPERTY = "hosts";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String MAX_CONNECTIONS_PROPERTY = "cassandra.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY = "cassandra.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY = "cassandra.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY = "cassandra.readtimeoutmillis";

  public static final String TRACING_PROPERTY = "cassandra.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "false";

  public static final String USE_SSL_CONNECTION = "cassandra.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  private static List<String> states = Arrays.asList("OPEN.DRAFT", "OPEN.PRESENTED", "CLOSED.SUBMITTED");
  private static Random random = new Random();
  private static AtomicLong counter = new AtomicLong();
  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static boolean debug = false;

  private static boolean trace = false;

  private ShoppingCartMapper mapper;

  private ShoppingCartDAO dao;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single // cluster/session instance for
    // all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized

      if (session != null) {
        return;
      }

      try {

        debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
        trace = Boolean.valueOf(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(
              String.format("Required property \"%s\" missing for CassandraCQLClient", HOSTS_PROPERTY));
        }
        String[] hosts = host.split(",");
        String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

        readConsistencyLevel = DefaultConsistencyLevel.valueOf(
            getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        writeConsistencyLevel = DefaultConsistencyLevel.valueOf(
            getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        List<InetSocketAddress> endpoints = new ArrayList<InetSocketAddress>();

        String datacenter = getProperties().getProperty("datacenter");
        for (String myHost : hosts) {
          InetSocketAddress address = new InetSocketAddress(myHost, Integer.valueOf(port));
          endpoints.add(address);
        }
        session = CqlSession.builder().addContactPoints(endpoints).withKeyspace(keyspace)
            .withLocalDatacenter(datacenter).build();
        mapper = new ShoppingCartMapperBuilder(session).build();
        dao = mapper.scDAO(CqlIdentifier.fromCql(keyspace));

      } catch (Exception e) {
        throw new DBException(e);
      }
    } // synchronized

  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        readStmts.clear();
        scanStmts.clear();
        insertStmts.clear();
        updateStmts.clear();
        readAllStmt.set(null);
        scanAllStmt.set(null);
        deleteStmt.set(null);
        session.close();
        /*
         * cluster.close(); cluster = null;
         */
        session = null;
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

    try {
      ShoppingCart cart = dao.get(key);
      result.put(key, new ByteArrayByteIterator(cart.toString().getBytes()));

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set
   *                    field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {

    int val = random.nextInt(2);

    PagingIterable<ShoppingCart> shoppingCartByState = null;
    if (val == 1) {
      shoppingCartByState = dao.getShoppingCartByState();
    } else {
      shoppingCartByState = dao.getShoppingCartByMultipleCriteria();
    }

    shoppingCartByState.forEach(cart -> extracted(result, cart));

    return Status.OK;
  }

  private void extracted(Vector<HashMap<String, ByteIterator>> result, ShoppingCart cart) {
    HashMap<String, ByteIterator> hashMap = new HashMap<>();
    hashMap.put(cart.getId(), new ByteArrayByteIterator(cart.toString().getBytes()));
    result.add(hashMap);
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record key,
   * overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      ShoppingCart cart = new ShoppingCart();
      cart.setExpiryDate(Instant.now(Clock.offset(Clock.systemDefaultZone(), Duration.ofDays(random.nextInt(30)))));
      cart.setModificationDate(
          Instant.now(Clock.offset(Clock.systemDefaultZone(), Duration.ofDays(random.nextInt(15)))));
      cart.setId(key);
      cart.setState(states.get(random.nextInt(3)));
      cart.setCustomerName("cust002" + random.nextInt(10000));
      dao.update(cart);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }

  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    try {
      ShoppingCart cart = new ShoppingCart();
      cart.setExpiryDate(Instant.now(Clock.offset(Clock.systemDefaultZone(), Duration.ofDays(random.nextInt(30)))));
      cart.setModificationDate(
          Instant.now(Clock.offset(Clock.systemDefaultZone(), Duration.ofDays(random.nextInt(15)))));
      cart.setId(key);
      cart.setState(states.get(random.nextInt(3)));
      cart.setCustomerName("cust002" + random.nextInt(10000));
      dao.save(cart);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {

    dao.delete(key);
    return Status.OK;
  }

}
