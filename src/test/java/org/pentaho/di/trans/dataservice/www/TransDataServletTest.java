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

package org.pentaho.di.trans.dataservice.www;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.www.SlaveServerConfig;
import org.pentaho.di.www.TransformationMap;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author bmorrise nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class TransDataServletTest extends BaseTest {

  private static final String BAD_CONTEXT_PATH = "/badsql";
  private static final String CONTEXT_PATH = "/sql";
  private static final String HEADER_SQL = "SQL";
  private static final String HEADER_MAX_ROWS = "MaxRows";
  private static final String TEST_SQL_QUERY = "SELECT * FROM dataservice_test";
  private static final String DEBUG_TRANS_FILE = "debugtransfile";
  private static final String TEST_MAX_ROWS = "100";
  private static final String PARAM_DEBUG_TRANS = "debugtrans";
  private static final String serviceTransUUID = UUID.randomUUID().toString();
  private static final String genTransUUID = UUID.randomUUID().toString();

  @Rule
  public TemporaryFolder fs = new TemporaryFolder();

  @Mock
  private HttpServletRequest request;

  @Mock
  private HttpServletResponse response;

  @Mock
  private TransformationMap transformationMap;

  @Mock
  private SlaveServerConfig slaveServerConfig;

  @Mock
  private Repository repository;

  @Mock
  private DelegatingMetaStore metaStore;

  @Mock
  private DataServiceClient.Query query;

  @Mock
  private ServletOutputStream outputStream;

  private Trans serviceTrans;

  private TransMeta genTransMeta;

  private Trans genTrans;

  @Mock
  private PrintWriter printWriter;

  private TransDataServlet servlet;
  private HashMultimap<String, String> headers;
  private HashMultimap<String, String> parameters;
  private File debugTrans;

  @Before
  public void setUp() throws Exception {
    serviceTrans = new Trans( transMeta );
    serviceTrans.setContainerObjectId( serviceTransUUID );
    genTransMeta = createTransMeta( TEST_SQL_QUERY );
    genTrans = new Trans( genTransMeta );
    genTrans.setContainerObjectId( genTransUUID );

    when( context.getDataServiceClient() ).thenReturn( client );

    servlet = new TransDataServlet( context );
    servlet.setJettyMode( true );
    servlet.setup( transformationMap, null, null, null );

    headers = HashMultimap.create();

    when( request.getHeaderNames() ).then( new Answer<Enumeration>() {
      @Override public Enumeration answer( InvocationOnMock invocation ) throws Throwable {
        return Collections.enumeration( headers.keySet() );
      }
    } );
    when( request.getHeaders( anyString() ) ).then( new Answer<Enumeration>() {
      @Override public Enumeration answer( InvocationOnMock invocation ) throws Throwable {
        return Collections.enumeration( headers.get( (String) invocation.getArguments()[0] ) );
      }
    } );
    when( request.getHeader( anyString() ) ).then( new Answer<String>() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        Iterator<String> value = headers.get( (String) invocation.getArguments()[0] ).iterator();
        return value.hasNext() ? value.next() : null;
      }
    } );

    parameters = HashMultimap.create();
    when( request.getParameterNames() ).then( new Answer<Enumeration>() {
      @Override public Enumeration answer( InvocationOnMock invocation ) throws Throwable {
        return Collections.enumeration( parameters.keySet() );
      }
    } );
    when( request.getParameter( anyString() ) ).then( new Answer<String>() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        Iterator<String> value = parameters.get( (String) invocation.getArguments()[0] ).iterator();
        return value.hasNext() ? value.next() : null;
      }
    } );
    when( request.getParameterValues( anyString() ) ).then( new Answer<String[]>() {
      @Override public String[] answer( InvocationOnMock invocation ) throws Throwable {
        Set<String> values = parameters.get( (String) invocation.getArguments()[0] );
        return values.toArray( new String[values.size()] );
      }
    } );

    when( request.getContextPath() ).thenReturn( CONTEXT_PATH );
    when( response.getOutputStream() ).thenReturn( outputStream );
    when( response.getWriter() ).thenReturn( printWriter );
    when( transformationMap.getSlaveServerConfig() ).thenReturn( slaveServerConfig );
    when( slaveServerConfig.getRepository() ).thenReturn( repository );
    when( slaveServerConfig.getMetaStore() ).thenReturn( metaStore );

    when( client.prepareQuery( (SQL) any(), anyInt(), anyMapOf( String.class, String.class ), (File) any() ) )
      .thenReturn( query );

    debugTrans = fs.newFile( DEBUG_TRANS_FILE );
  }

  @Test
  public void testDoPut() throws Exception {
    when( request.getMethod() ).thenReturn( "PUT" );

    headers.put( HEADER_SQL, TEST_SQL_QUERY );
    headers.put( HEADER_MAX_ROWS, TEST_MAX_ROWS );
    parameters.put( "PARAMETER_FOO", "BAR" );

    parameters.put( PARAM_DEBUG_TRANS, debugTrans.getPath() );

    when( query.getTransList() ).thenReturn( ImmutableList.of( serviceTrans, genTrans ) );

    servlet.service( request, response );

    verify( client ).setRepository( repository );
    verify( client ).setMetaStore( metaStore );

    verify( client ).prepareQuery(
      argThat( isSqlFor( TEST_SQL_QUERY ) ),
      eq( Integer.parseInt( TEST_MAX_ROWS ) ),
      eq( ImmutableMap.of( "FOO", "BAR" ) ),
      eq( debugTrans )
    );

    verify( transformationMap ).addTransformation(
      eq( DATA_SERVICE_NAME ), eq( serviceTransUUID ), eq( serviceTrans ),
      (TransConfiguration) argThat( hasProperty( "transMeta", is( transMeta ) ) )
    );
    verify( transformationMap ).addTransformation(
      eq( TEST_SQL_QUERY ), eq( genTransUUID ), eq( genTrans ),
      (TransConfiguration) argThat( hasProperty( "transMeta", is( genTransMeta ) ) )
    );

    verify( response ).setStatus( HttpServletResponse.SC_OK );
    verify( response ).setContentType( "binary/jdbc" );
    verify( query ).writeTo( outputStream );
  }

  @Test
  public void testQueryNotGiven() throws Exception {
    headers.removeAll( HEADER_SQL );
    servlet.service( request, response );

    verify( response ).setStatus( HttpServletResponse.SC_BAD_REQUEST );
  }

  @Test
  public void testDoGetException() throws Exception {
    headers.put( HEADER_SQL, TEST_SQL_QUERY );
    when( request.getMethod() ).thenReturn( "PUT" );
    KettleException kettleException = new KettleException( "Expected exception" );
    when( client.prepareQuery(
      argThat( isSqlFor( TEST_SQL_QUERY ) ),
      eq( -1 ),
      eq( ImmutableMap.<String, String>of() ),
      (File) isNull() )
    ).thenThrow( kettleException );

    servlet.service( request, response );

    verify( response ).setStatus( HttpServletResponse.SC_INTERNAL_SERVER_ERROR );
    verify( printWriter ).println( contains( "Expected exception" ) );
  }

  @Test
  public void testDoGetBadPath() throws Exception {
    when( request.getContextPath() ).thenReturn( BAD_CONTEXT_PATH );

    servlet.service( request, response );

    verify( response, never() ).setStatus( HttpServletResponse.SC_OK );
  }

  @Test
  public void testGetContextPath() {
    assertEquals( CONTEXT_PATH, servlet.getContextPath() );
  }
}
