package com.yahoo.ycsb.db;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

/**
 * TeSt.
 * @author eprafpr
 *
 */
@Mapper
public interface ShoppingCartMapper {

  @DaoFactory
  ShoppingCartDAO scDAO(@DaoKeyspace CqlIdentifier keyspace);
}
