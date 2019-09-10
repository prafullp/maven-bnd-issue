package com.yahoo.ycsb.db;

import java.time.Instant;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

/**
 * Test.
 * @author eprafpr
 *
 */
@Entity
@CqlName("shoppingcart")
public class ShoppingCart {

  @PartitionKey
  private String id;

  private String state;

  @CqlName("modificationdate")
  private Instant modificationDate;

  @CqlName("expirydate")
  private Instant expiryDate;

  @CqlName("customername")
  private String customerName;

  public String getCustomerName() {
    return customerName;
  }

  public void setCustomerName(String custName) {
    this.customerName = custName;
  }

  public String getId() {
    return id;
  }

  public void setId(String identifier) {
    this.id = identifier;
  }

  public String getState() {
    return state;
  }

  public void setState(String theState) {
    this.state = theState;
  }

  public Instant getModificationDate() {
    return modificationDate;
  }

  public void setModificationDate(Instant theModificationDate) {
    this.modificationDate = theModificationDate;
  }

  public Instant getExpiryDate() {
    return expiryDate;
  }

  public void setExpiryDate(Instant theExpiryDate) {
    this.expiryDate = theExpiryDate;
  }

}
