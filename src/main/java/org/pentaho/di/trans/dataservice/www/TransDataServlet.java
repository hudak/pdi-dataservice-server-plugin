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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.www.BaseCartePlugin;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This servlet allows a user to get data from a "service" which is a transformation step.
 *
 * @author matt
 */
@CarteServlet(
  id = "sql",
  name = "Get data from a data service",
  description = "Get data from a transformation data service using SQL"
)
public class TransDataServlet extends BaseCartePlugin {
  public static final String PARAMETER_PREFIX = "PARAMETER_";
  public static final String MAX_ROWS = "MaxRows";
  private static Class<?> PKG = TransDataServlet.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private static final long serialVersionUID = 3634806745372015720L;

  public static final String CONTEXT_PATH = "/sql";
  private final DataServiceClient client;

  public TransDataServlet( DataServiceContext context ) {
    client = context.getDataServiceClient();
    log = context.getLogChannel();
  }

  @Override public void handleRequest( CarteRequest request ) throws IOException {
    SQL sql;
    try {
      sql = new SQL( checkNotNull( request.getHeader( "SQL" ), "Query not given" ) );
    } catch ( Exception e ) {
      log.logError( "Error parsing SQL", e );
      request.respond( 400 ).withMessage( "Invalid SQL Query: " + e.getMessage() );
      return;
    }

    // Collect parameters from the request to pass to the service trans
    Map<String, String> parameters = Maps.newHashMap();
    collectParameters( parameters, request.getParameters() );

    // Get maximum rows, or -1 for all rows
    int maxRows = -1;
    if ( request.getHeaders().containsKey( MAX_ROWS ) ) {
      maxRows = firstNonNull( Ints.tryParse( request.getHeader( MAX_ROWS ) ), maxRows );
    }

    // Log the generated transformation if needed
    String debugTrans = request.getParameter( "debugtrans" );
    File debugTransFile = !Strings.isNullOrEmpty( debugTrans ) ? new File( debugTrans ) : null;

    final DataServiceClient.Query query;
    try {
      // Update client with configured repository and metaStore
      client.setRepository( transformationMap.getSlaveServerConfig().getRepository() );
      client.setMetaStore( transformationMap.getSlaveServerConfig().getMetaStore() );

      // Pass query to client
      query = client.prepareQuery( sql, maxRows, parameters, debugTransFile );
    } catch ( KettleException e ) {
      log.logError( "Error executing query: " + sql, e );
      request.respond( 500 ).withMessage( "Error executing query: " + e.getMessage() );
      return;
    }

    // For logging and tracking purposes, let's expose both the service transformation as well
    // as the generated transformation on this very carte instance
    //
    for ( Trans trans : query.getTransList() ) {
      monitorTransformation( trans );
    }

    // Execute query response
    request.respond( 200 ).with( "binary/jdbc", new OutputStreamResponse() {
      @Override public void write( OutputStream outputStream ) throws IOException {
        query.writeTo( outputStream );
      }
    } );
  }

  private void monitorTransformation( Trans trans ) {
    TransMeta transMeta = trans.getTransMeta();
    TransExecutionConfiguration executionConfiguration = new TransExecutionConfiguration();
    TransConfiguration config = new TransConfiguration( transMeta, executionConfiguration );
    transformationMap .addTransformation( transMeta.getName(), trans.getContainerObjectId(), trans, config );
  }

  private Map<String, String> collectParameters( Map<String, String> parameters,
                                                 Map<String, Collection<String>> map ) {
    for ( Map.Entry<String, Collection<String>> parameterEntry : map.entrySet() ) {
      String name = parameterEntry.getKey();
      Iterator<String> value = parameterEntry.getValue().iterator();
      if ( name.startsWith( PARAMETER_PREFIX ) && value.hasNext() ) {
        parameters.put( name.substring( PARAMETER_PREFIX.length() ), value.next() );
      }
    }
    return parameters;
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
