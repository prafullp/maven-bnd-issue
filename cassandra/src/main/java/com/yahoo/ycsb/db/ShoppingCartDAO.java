package com.yahoo.ycsb.db;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;
import com.datastax.oss.driver.api.mapper.annotations.Update;

/**
 * Test.
 * 
 * @author eprafpr
 *
 */
@Dao
public interface ShoppingCartDAO {

  @Insert
  @StatementAttributes(consistencyLevel = "LOCAL_QUORUM")
  void save(ShoppingCart shoppingCart);

  @Update
  @StatementAttributes(consistencyLevel = "LOCAL_QUORUM")
  void update(ShoppingCart shoppingCart);

  @Select
  @StatementAttributes(consistencyLevel = "LOCAL_QUORUM")
  ShoppingCart get(String key);

  @Delete(entityClass = ShoppingCart.class)
  @StatementAttributes(consistencyLevel = "LOCAL_QUORUM")
  void delete(String key);

  @Query("select * from ${keyspaceId}.${tableId} where"
      + " expr(sc_index, '{filter: {type: \"match\", field: \"state\", value: \"OPEN.DRAFT\"}}') limit 1")
  @StatementAttributes(consistencyLevel = "LOCAL_QUORUM")
  PagingIterable<ShoppingCart> getShoppingCartByState();
  
  @Query("select * from ${keyspaceId}.${tableId} where"
      + " expr(sc_index, '{filter: "
      + "[{type: \"range\", field: \"modificationdate\", lower: \"2019-08-18 10:10:10\", upper: \"2019-08-28 10:10:10\""
      + "},{type: \"range\", field: \"expirydate\", lower: \"2019-08-20 10:10:10\", upper: \"2019-08-31 10:10:10\"},"
      + "{type: \"match\", field: \"state\", value: \"OPEN.DRAFT\"}]"
      + "}') limit 1")
  @StatementAttributes(consistencyLevel = "LOCAL_QUORUM")
  PagingIterable<ShoppingCart> getShoppingCartByMultipleCriteria();
  
}
