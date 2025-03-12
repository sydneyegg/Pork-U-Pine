package com.acme.basic;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

  private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

  public static void main(String[] args) {
    for (int left = 0; left < 10; left++) {
      for (int right = 0; right < 10; left++) {
        LOGGER.log(Level.INFO, "Pair: ({0},{1})", new Object[] {left, right});
      }
    }
  }

}