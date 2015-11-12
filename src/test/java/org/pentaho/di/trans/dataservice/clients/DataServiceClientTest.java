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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.jdbc.ThinResultFactory;
import org.pentaho.di.trans.dataservice.jdbc.ThinResultSet;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.dataservice.testing.answers.ReturnsSelf.RETURNS_SELF;

/**
 * @author bmorrise
 */
@RunWith( MockitoJUnitRunner.class )
public class DataServiceClientTest extends BaseTest {

  private static final String DUAL_TABLE_NAME = "dual";
  private static final String TEST_DUMMY_SQL_QUERY = "SELECT 1";
  private static final String TEST_SQL_QUERY = "SELECT * FROM " + DATA_SERVICE_NAME;
  private static final int MAX_ROWS = 100;

  @Rule
  public TemporaryFolder fs = new TemporaryFolder();

  @Mock
  private Repository repository;

  @Mock
  private IMetaStore metaStore;

  private DataServiceExecutor.Builder builder;

  @Mock
  private DataServiceExecutor executor;

  @Mock
  private RowMetaInterface rowMetaInterface;

  @Mock
  private Trans serviceTrans;

  @Mock
  private Trans genTrans;
  private File debugTrans;
  public static final String GEN_TRANS_XML = "<transMeta mock=genTrans/>";
  private TransMeta genTransMeta;
  private ThinResultFactory resultFactory;

  @Before
  public void setUp() throws Exception {
    when( metaStoreUtil.getDataService( DATA_SERVICE_NAME, repository, metaStore ) ).thenReturn( dataService );

    builder = mock( DataServiceExecutor.Builder.class, RETURNS_SELF );
    when( context.createBuilder( argThat( isSqlFor( TEST_SQL_QUERY ) ), same( dataService ) ) ).thenReturn( builder );
    doReturn( executor ).when( builder ).build();
    when( executor.executeQuery( ( (DataOutputStream) any() ) ) ).thenReturn( executor );
    when( executor.getServiceTrans() ).thenReturn( serviceTrans );
    when( executor.getGenTrans() ).thenReturn( genTrans );
    genTransMeta = createTransMeta( TEST_SQL_QUERY );
    when( executor.getGenTransMeta() ).thenReturn( genTransMeta );

    debugTrans = fs.newFile();
    doReturn( GEN_TRANS_XML ).when( genTransMeta ).getXML();

    client = new DataServiceClient( context );
    client.setMetaStore( metaStore );
    client.setRepository( repository );
    resultFactory = new ThinResultFactory();
  }

  @Test
  public void testQuery() throws Exception {
    assertNotNull( client.query( TEST_SQL_QUERY, MAX_ROWS ) );
    verify( builder ).rowLimit( MAX_ROWS );
    verify( executor ).waitUntilFinished();

    assertNotNull( client.query( TEST_DUMMY_SQL_QUERY, MAX_ROWS ) );
    verifyNoMoreInteractions( ignoreStubs( executor ) );
    verify( logChannel, never() ).logError( anyString(), any( Throwable.class ) );

    MetaStoreException exception = new MetaStoreException();
    when( metaStoreUtil.getDataService( DATA_SERVICE_NAME, repository, metaStore ) ).thenThrow( exception );
    try {
      assertThat( client.query( TEST_SQL_QUERY, MAX_ROWS ), not( anything() ) );
    } catch ( SQLException e ) {
      assertThat( Throwables.getCausalChain( e ), hasItem( exception ) );
    }
  }

  @Test
  public void testPrepareExecution() throws Exception {
    DataServiceClient.Query query;

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SQL sql = new SQL( "SELECT *" );
    query = client.prepareQuery( sql, -1 );
    assertThat( sql.getServiceName(), equalTo( DataServiceClient.DUMMY_TABLE_NAME ) );
    assertThat( query.getTransList(), emptyCollectionOf( Trans.class ) );

    query.writeTo( outputStream );
    DataInputStream inputStream = new DataInputStream( new ByteArrayInputStream( outputStream.toByteArray() ) );
    ThinResultSet resultSet = resultFactory.loadResultSet( inputStream );
    assertThat( resultSet.next(), is( true ) );
    assertThat( resultSet.getMetaData().getColumnCount(), equalTo( 1 ) );
    assertThat( resultSet.getMetaData().getColumnLabel( 1 ), equalTo( "DUMMY" ) );
    assertThat( resultSet.getString( 1 ), equalTo( "x" ) );
    assertThat( resultSet.next(), is( false ) );

    sql = new SQL( TEST_SQL_QUERY );
    ImmutableMap<String, String> parameters = ImmutableMap.of( "myParam", "value" );
    query = client.prepareQuery( sql, MAX_ROWS, parameters, debugTrans );
    verify( builder ).rowLimit( MAX_ROWS );
    verify( builder ).parameters( parameters );
    verify( builder ).build();
    verify( executor, never() ).executeQuery( (DataOutputStream) any() );
    verify( executor, never() ).executeQuery( (RowListener) any() );
    assertThat( query.getTransList(), contains( serviceTrans, genTrans ) );

    verify( logChannel, never() ).logError( anyString(), (Exception) any() );
  }

  @Test
  public void testSaveGeneratedTrans() throws Exception {
    client.saveGeneratedTransformation( genTransMeta, debugTrans );
    assertThat( Files.readLines( debugTrans, Charsets.UTF_8 ), hasItem( GEN_TRANS_XML ) );
    verify( logChannel, never() ).logError( anyString(), (Exception) any() );

    // Log error if save fails, do not propagate
    client.saveGeneratedTransformation( genTransMeta, fs.getRoot() );
    verify( logChannel ).logError( anyString(), isA( FileNotFoundException.class ) );
  }

  @Test
  public void testGetServiceInformation() throws Exception {
    when( transMeta.getStepFields( dataService.getStepname() ) ).thenReturn( rowMetaInterface );

    when( metaStoreUtil.logErrors( anyString() ) ).thenReturn( exceptionHandler );
    when( metaStoreUtil.getDataServices( repository, metaStore, exceptionHandler ) )
      .thenReturn( ImmutableList.of( dataService ) );

    assertThat( client.getServiceInformation(), contains( allOf(
      hasProperty( "name", equalTo( DATA_SERVICE_NAME ) ),
      hasProperty( "serviceFields", equalTo( rowMetaInterface ) )
    ) ) );
    verify( transMeta ).activateParameters();

    when( transMeta.getStepFields( DATA_SERVICE_STEP ) ).thenThrow( new KettleStepException() );
    assertThat( client.getServiceInformation(), is( empty() ) );
  }

}
