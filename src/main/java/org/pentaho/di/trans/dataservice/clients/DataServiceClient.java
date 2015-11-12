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

package org.pentaho.di.trans.dataservice.clients;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.metastore.api.IMetaStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

public class DataServiceClient implements DataServiceClientService {
  private final DataServiceMetaStoreUtil metaStoreUtil;
  private final DataServiceContext context;

  private Repository repository;
  private IMetaStore metaStore;

  public static final String DUMMY_TABLE_NAME = "dual";

  public DataServiceClient( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
    this.context = context;
  }

  @Override public DataInputStream query( String sqlQuery, final int maxRows ) throws SQLException {
    DataInputStream dataInputStream;

    try {

      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

      prepareQuery( new SQL( sqlQuery ), maxRows ).writeTo( byteArrayOutputStream );

      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
      dataInputStream = new DataInputStream( byteArrayInputStream );

    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, SQLException.class );
      throw new SQLException( e );
    }

    return dataInputStream;
  }

  public Query prepareQuery( SQL sql, int maxRows ) throws KettleException {
    return prepareQuery( sql, maxRows, ImmutableMap.<String, String>of(), null );
  }

  public Query prepareQuery( SQL sql, int maxRows, Map<String, String> parameters, File debugGenTrans )
    throws KettleException {
    if ( sql.getServiceName() == null || sql.getServiceName().equals( DUMMY_TABLE_NAME ) ) {
      // Support for SELECT 1 and SELECT 1 FROM dual
      sql.setServiceName( DUMMY_TABLE_NAME );
      return new DualQuery( sql );
    } else {
      // Locate data service
      DataServiceMeta dataService = findDataService( sql );

      DataServiceExecutor executor = context.createBuilder( sql, dataService )
        .rowLimit( maxRows )
        .parameters( parameters )
        .build();
      if ( debugGenTrans != null ) {
        saveGeneratedTransformation( executor.getGenTransMeta(), debugGenTrans );
      }
      return new ExecutorQuery( executor );
    }
  }

  protected void saveGeneratedTransformation( TransMeta genTransMeta, File debugTrans ) {
    try {
      FileOutputStream fos = new FileOutputStream( debugTrans );
      fos.write( XMLHandler.getXMLHeader( Const.XML_ENCODING ).getBytes( Const.XML_ENCODING ) );
      fos.write( genTransMeta.getXML().getBytes( Const.XML_ENCODING ) );
      fos.close();
    } catch ( Exception e ) {
      context.getLogChannel().logError( "Unable to write dynamic transformation to file", e );
    }
  }

  public void writeDummyRow( SQL sql, DataOutputStream dos ) throws IOException {
    sql.setServiceName( DUMMY_TABLE_NAME );

    DataServiceExecutor.writeMetadata( dos, new String[] { DUMMY_TABLE_NAME, "", "", "", "" } );

    try {
      RowMetaInterface rowMeta = new RowMeta();
      rowMeta.addValueMeta( new ValueMetaString( "DUMMY" ) );
      rowMeta.writeMeta( dos );

      Object[] row = new Object[] { "x" };
      rowMeta.writeData( dos, row );
    } catch ( KettleFileException e ) {
      throw new IOException( e );
    }
  }

  private DataServiceMeta findDataService( SQL sql ) throws KettleException {
    try {
      return metaStoreUtil.getDataService( sql.getServiceName(), repository, metaStore );
    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, KettleException.class );
      throw new KettleException( "Unable to locate data service", e );
    }
  }

  @Override public List<ThinServiceInformation> getServiceInformation() throws SQLException {
    List<ThinServiceInformation> services = Lists.newArrayList();

    for ( DataServiceMeta service : metaStoreUtil.getDataServices( repository, metaStore, logErrors() ) ) {
      TransMeta transMeta = service.getServiceTrans();
      try {
        transMeta.activateParameters();
        RowMetaInterface serviceFields = transMeta.getStepFields( service.getStepname() );
        ThinServiceInformation serviceInformation = new ThinServiceInformation( service.getName(), serviceFields );
        services.add( serviceInformation );
      } catch ( Exception e ) {
        String message = MessageFormat.format( "Unable to get fields for service {0}, transformation: {1}",
          service.getName(), transMeta.getName() );
        context.getLogChannel().logError( message, e );
      }
    }

    return services;
  }

  private Function<Exception, Void> logErrors() {
    return metaStoreUtil.logErrors( "Unable to retrieve data service" );
  }

  public void setRepository( Repository repository ) {
    this.repository = repository;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  private DataOutputStream asDataOutputStream( OutputStream outputStream ) {
    return outputStream instanceof DataOutputStream ?
      ( (DataOutputStream) outputStream ) :
      new DataOutputStream( outputStream );
  }

  public interface Query {

    void writeTo( OutputStream outputStream ) throws IOException;

    List<Trans> getTransList();

  }

  private class DualQuery implements Query {
    private final SQL sql;

    public DualQuery( SQL sql ) {
      this.sql = sql;
    }

    @Override public void writeTo( OutputStream outputStream ) throws IOException {
      writeDummyRow( sql, asDataOutputStream( outputStream ) );
    }

    @Override public List<Trans> getTransList() {
      return ImmutableList.of();
    }
  }

  private class ExecutorQuery implements Query {
    private final DataServiceExecutor executor;

    public ExecutorQuery( DataServiceExecutor executor ) {
      this.executor = executor;
    }

    public void writeTo( OutputStream outputStream ) throws IOException {
      executor.executeQuery( asDataOutputStream( outputStream ) ).waitUntilFinished();
    }

    @Override public List<Trans> getTransList() {
      return ImmutableList.of( executor.getServiceTrans(), executor.getGenTrans() );
    }
  }

}
