/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.optimization;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.execution.ExecutionPoint;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaDataCombi;

/**
 * @author nhudak
 */
public abstract class StepOptimization implements PushDownType {

  public ListenableFuture<Boolean> activate( final DataServiceExecutor executor,
                                             final PushDownOptimizationMeta meta ) {
    Task task = new Task( executor, meta );
    executor.addTask( task );
    return task;
  }

  @Override public OptimizationImpactInfo preview( DataServiceExecutor executor, PushDownOptimizationMeta meta ) {
    OptimizationImpactInfo info = new OptimizationImpactInfo( meta.getStepName() );
    Trans serviceTrans = executor.getServiceTrans();
    try {
      // Start up service Trans threads
      serviceTrans.prepareExecution( null );

      // Find the step thread and run preview
      info = preview( executor, serviceTrans.findRunThread( meta.getStepName() ) );

      // Dispose resources
      for ( StepMetaDataCombi stepMetaDataCombi : serviceTrans.getSteps() ) {
        stepMetaDataCombi.step.setOutputDone();
        stepMetaDataCombi.step.dispose( stepMetaDataCombi.meta, stepMetaDataCombi.data );
        stepMetaDataCombi.step.markStop();
      }
    } catch ( Exception e ) {
      info.setErrorMsg( e.getMessage() );
    }
    return info;
  }

  protected abstract boolean activate( DataServiceExecutor executor, StepInterface stepInterface );

  protected abstract OptimizationImpactInfo preview( DataServiceExecutor executor, StepInterface stepInterface );

  public class Task extends AbstractFuture<Boolean> implements ExecutionPoint {
    private final DataServiceExecutor executor;
    private final PushDownOptimizationMeta meta;

    public Task( DataServiceExecutor executor, PushDownOptimizationMeta meta ) {
      this.executor = executor;
      this.meta = meta;
    }

    @Override public void run() {
      try {
        StepInterface stepInterface = executor.getServiceTrans().findRunThread( meta.getStepName() );
        set( stepInterface != null && activate( executor, stepInterface ) );
      } catch ( Exception e ) {
        setException( e );
      }
    }

    @Override public double getPriority() {
      return OPTIMIZE;
    }
  }

}
