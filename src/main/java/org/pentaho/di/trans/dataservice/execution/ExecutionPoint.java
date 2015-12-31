package org.pentaho.di.trans.dataservice.execution;

import com.google.common.base.Function;

/**
 * @author nhudak
 */
public interface ExecutionPoint extends Runnable {
  double PREPARE = 0, OPTIMIZE = 1, READY = 2, START = 3, CLEANUP = 4;
  Function<ExecutionPoint, Double> PRIORITY = new Function<ExecutionPoint, Double>() {
    @Override public Double apply( ExecutionPoint input ) {
      return input.getPriority();
    }
  };

  double getPriority();

  void run();
}
