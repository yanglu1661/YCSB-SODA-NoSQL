//YangLu created 2022/04/15 for Oracle NoSQL database
package com.yahoo.ycsb.db;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Vector;
import java.util.Properties;
import java.sql.SQLException;
//import org.json.simple.JSONObject;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


import oracle.nosql.driver.Region;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.iam.SignatureProvider;

/**
 * nosql adapter for YCSB. 
 */
public class nosqlClient extends DB {
    
    private NoSQLHandle _handle;
    private boolean _table_created = false;
    
    //private String table_name;
  private String _OCI_CONFIG="/home/opc/.oci/config";
    
  public nosqlClient() {

  }
  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    //String usr = props.getProperty("nosql.user");
    //String pwd = props.getProperty("nosql.password");
    //String db_schema = props.getProperty("nosql.db");
    String oci_config = props.getProperty("nosql.oci_config");
    String service_region = props.getProperty("nosql.region");
    String endpoint = null;
    if( service_region == null)
    { 
      endpoint= props.getProperty("nosql.url");
      if (endpoint == null) {
        throw new IllegalStateException("nosql.url not specified");
      }
    }
    int batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));
    if (batchSize != 1) {
      throw new UnsupportedOperationException(); // todo
    }

    try {
        /* Set up the handle configuration */
        System.out.println("connecting endpoint: " + endpoint);
        System.out.println("oci_config: " + oci_config);
        
        //if (endpoint.startsWith("nosql.")) {

        System.out.println("cloud signature: " + oci_config);

        Region region = Region.fromRegionId(service_region);
        if (region != null) {
            endpoint = region.endpoint();
        }
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);

        if (oci_config != null) {
              config.setAuthorizationProvider(new SignatureProvider(oci_config, "DEFAULT"));
        }
        else{

                /* default looks for the file, $HOME/.oci/config */
                    //config.setAuthorizationProvider(new SignatureProvider());
                //}
        //}
        //else {        
        //System.out.println("OP version: " );
            config.setAuthorizationProvider(new StoreAccessTokenProvider());
        }
        _handle = NoSQLHandleFactory.createNoSQLHandle(config);
        System.out.println("Handler: " +_handle);

        if( _table_created)
        {
        String tbl = props.getProperty("table");
         final String createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tbl +
            //"(id String NOT NULL DEFAULT \"0\", " +
            "(id String , " +
            " document JSON, " +
            " PRIMARY KEY(id))";     
            
        TableRequest tableRequest = new TableRequest()
            .setStatement(createTableStatement)
            .setTableLimits(new TableLimits(100000, 40000, 50))
            ;
        //System.out.println("Creating table " + tableName);
        /* this call will succeed or throw an exception */
        _handle.doTableRequest(tableRequest,
                              60000, /* wait up to 60 sec */
                              1000); /* poll once per second */
        System.out.println("Created table _handle:" + _handle);
        _table_created = true;
        }
      
    } catch (Exception e) {
      throw new DBException(e);
    } 
  }
  @Override
  public void cleanup() throws DBException {
    try {
        System.out.println("close handle:" + _handle);
      _handle.close();
    } catch (Exception e) {
      throw new DBException(e);
    }
  }
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
        MapValue keys = new MapValue().put("id", key);
        GetRequest getRequest = new GetRequest()
            .setKey(keys)
            .setTableName(table);
        GetResult res = _handle.get(getRequest);
        //System.out.println("Got row: " + getRes.getValue());

      //Collection collection = db.getCollection(table);
      //DbDoc res = collection.getOne(key);
      
      if (fields != null && !fields.isEmpty()) {
        throw new UnsupportedOperationException(); // todo
      }
      if (res == null) {
        throw new IllegalStateException("read returned no document:" + key);
      } else  {
        documentToMap(res, result);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      //Collection collection = db.getCollection(table);
      
        String jsonString = mapToDocument(key, values, false);
        
        MapValue value = new MapValue()
            .put("id", key) // fill in cookie_id field
            .put("document",  jsonString);
        

        PutRequest putRequest = new PutRequest()
            .setOption( PutRequest.Option.IfPresent )
            .setValue(value)
            //.setValueFromJson(jsonString, null)
            .setTableName(table);

        PutResult putRes = _handle.put(putRequest);
        //System.out.println("Put row: " + value + " result=" + putRes);


      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  @Override
  public Status delete(String table, String key) {
    try {
        MapValue key_id = new MapValue().put("id", key);
        DeleteRequest delRequest = new DeleteRequest()
            .setKey(key_id)
            .setTableName(table);

        DeleteResult del = _handle.delete(delRequest);
        //System.out.println("Deleted key " + key + " result=" + del);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {

        String jsonString = mapToDocument(key, values, false);

        MapValue value = new MapValue()
            .put("id", key) // fill in cookie_id field
            .put("document",  jsonString);



      if (jsonString == null) {
        throw new IllegalStateException("Inserting a null doc");
      }
        PutRequest putRequest = new PutRequest()
            .setOption( PutRequest.Option.IfAbsent )
            .setValue(value)
            //.setValueFromJson(jsonString, null)
            .setTableName(table);

        PutResult putRes = _handle.put(putRequest);

        //System.out.println("Put row: " + value + " result=" + putRes);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  //nosql API seems not support scan operation, just skip it.
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
  return Status.OK;
  }
  private String mapToDocument(String key, Map<String, ByteIterator> values, boolean useBinary)  {
     
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode json = mapper.createObjectNode(); 

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
       //json.put(entry.getKey(),  new String(entry.getValue().toArray()),"UTF-8");
       json.put(entry.getKey(),  entry.getValue().toString());
    }
    
    return json.toString();
    //return testJsonStr;
  }

  private void documentToMap(GetResult res, Map<String, ByteIterator> result) {

   try {
    String json = res.getJsonValue();
    Map<String, String> map = new HashMap<String, String>();
    ObjectMapper mapper = new ObjectMapper();
    map = mapper.readValue(json, new TypeReference<HashMap<String,String>>(){});
    for (String key : map.keySet()) {
        String value = map.get(key);
        result.put(key, new StringByteIterator(value));
    }
   }
   catch(Exception e) {
    e.printStackTrace();
   } 
  }  
  
}
