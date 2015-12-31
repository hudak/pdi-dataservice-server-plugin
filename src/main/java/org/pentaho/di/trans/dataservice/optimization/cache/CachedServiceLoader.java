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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.execution.DefaultTransWiring;
import org.pentaho.di.trans.dataservice.execution.ExecutionPoint;
import org.pentaho.di.trans.dataservice.execution.TransStarter;
import org.pentaho.di.trans.step.StepMetaDataCombi;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author nhudak
 */
class CachedServiceLoader implements ExecutionPoint {
  private final Executor executor;
  private final CachedService cachedService;
  private ListenableFutureTask<Integer> replay;

  CachedServiceLoader( CachedService cachedService, Executor executor ) {
    this.cachedService = cachedService;
    this.executor = executor;
  }

  public ListenableFuture<Integer> replay( DataServiceExecutor dataServiceExecutor ) throws KettleException {
    final Trans serviceTrans = dataServiceExecutor.getServiceTrans(), genTrans = dataServiceExecutor.getGenTrans();
    final RowProducer rowProducer = dataServiceExecutor.addRowProducer();

    Iterables.removeIf( dataServiceExecutor.getTasks(), Predicates.or(
      Predicates.instanceOf( DefaultTransWiring.class ),
      new Predicate<Runnable>() {
        @Override public boolean apply( Runnable runnable ) {
          return runnable instanceof TransStarter && ( (TransStarter) runnable ).getTrans().equals( serviceTrans );
        }
      }
    ) );

    dataServiceExecutor.addTask( ExecutionPoint.READY, new Runnable() {
      @Override public void run() {
        serviceTrans.stopAll();
        for ( StepMetaDataCombi stepMetaDataCombi : serviceTrans.getSteps() ) {
          stepMetaDataCombi.step.setOutputDone();
          stepMetaDataCombi.step.dispose( stepMetaDataCombi.meta, stepMetaDataCombi.data );
          stepMetaDataCombi.step.markStop();
        }
      }
    } );

    replay = ListenableFutureTask.create( new Callable<Integer>() {
      @Override public Integer call() throws Exception {
        int rowCount = 0;
        for ( Iterator<RowMetaAndData> iterator = cachedService.getRowMetaAndData().iterator();
              iterator.hasNext() && genTrans.isRunning(); ) {
          RowMetaAndData metaAndData = iterator.next();
          boolean rowAdded = false;
          RowMetaInterface rowMeta = metaAndData.getRowMeta();
          Object[] rowData = rowMeta.cloneRow( metaAndData.getData() );
          while ( !rowAdded && genTrans.isRunning() ) {
            rowAdded = rowProducer.putRowWait( rowMeta, rowData, 10, TimeUnit.SECONDS );
          }
          if ( rowAdded ) {
            rowCount += 1;
          }
        }
        rowProducer.finished();
        return rowCount;
      }
    } );

    dataServiceExecutor.addTask( this );
    return replay;
  }

  @Override public double getPriority() {
    return ExecutionPoint.START;
  }

  @Override public void run() {
    // Start cache replay
    executor.execute( replay );
  }
}
