package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class SchemaRegistryProducer extends AbstractKafkaProducer {
    static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryProducer.class);

    private final KafkaProducer<String, GenericRecord> kafka;
    private String schemaMappingURI;

    private static final String LOOKUP_KEY = "key_column";
    private static final String LOOKUP_DATA = "data_column";
    private static final String LOOKUP_SCHEMA = "schema";
    private static final String LOOKUP_METADATA = "metadata";
    private static final String LOOKUP_HAS_NESTED_SCHEMA = "has_child_schemas";
    private static final String LOOKUP_CHILD_SCHEMA = "child_schema_uri";

    private static final String FIELDS = "fields";
    private static final String NAME = "name";
    private static final String TYPE = "type";

    private static final String JSON_SUFFIX = ".json";

    private static final String TYPE_NULL = "null";
    private static final String TYPE_DOUBLE = "double";
    private static final String TYPE_FLOAT = "float";
    private static final String TYPE_LONG = "long";
    private static final String TYPE_STRING = "string";
    private static final String TYPE_INT = "int";
    private static final String TYPE_BOOLEAN = "boolean";

    // NOTE: The KafkaAvroProducer maintains a cache of known childSchemas via an IdentityHashMap.
    // Since the equals method on IdentityHashMap is based on identity, not value - like other hashmaps,
    // passing in a newly created Schema that is logically equivalent to previously passed in childSchemas
    // results in the Producer believing it has a new Schema and caching it (again).
    // As a result the Producer's cache quickly fills up. Once full the Producer raises and exception and falls down.
    // Given this behavior we must maintain our own cache of childSchemas and always pass in the same Schema object to the
    // Producer. This represents a little more overhead, but it should be tolerable - even a few hundred Schemas would
    // represent a small about of in-memory data.
    private HashMap<String, Schema> schemaCache = new HashMap<>();

    // a cache of URIs to their data
    private HashMap<String, JSONObject> resourceCache = new HashMap<>();

    // hardcoded ddl schema in case we decide we want to process ddl
    private static final String ddlSchemaString = "{" +
                                                "      \"type\": \"record\"," +
                                                "      \"name\": \"maxwell_ddl\"," +
                                                "      \"namespace\": \"maxwell_dll.avro\"," +
                                                "      \"fields\": [" +
                                                "        {" +
                                                "          \"name\": \"value\"," +
                                                "          \"type\": \"string\"" +
                                                "        }" +
                                                "    ]" +
                                                "}";

    private static final Schema ddlSchema = new Schema.Parser().parse(ddlSchemaString);
    private static final GenericRecord ddlSchemaRecord = new GenericData.Record(ddlSchema);

    public SchemaRegistryProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
        super(context, kafkaTopic);

        // hardcoded because the schema registry is opinionated about what it accepts
        kafkaProperties.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProperties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        this.kafka = new KafkaProducer<>(kafkaProperties);

        this.schemaMappingURI = context.getConfig().schemaMappingURI;
    }

    /**
     * Load a resource from the given URL
     *
     * @param resourceURI
     * @return the JSONObject specified by the URI
     */
    private JSONObject loadResource(String resourceURI) {
        if (resourceURI == null) {
            throw new RuntimeException("No resource supplied.");
        }

        try {
            URL resource = new URL(resourceURI);

            String content = IOUtils.toString(resource.openStream()); // TODO: deprecated call
            return new JSONObject(content);
        } catch (IOException | NullPointerException e) {
            throw new RuntimeException("Failed to load resource: " + resourceURI + ". Exception: " + e.toString());
        }
    }

    /**
     * Assemble the schema for a given rowMap. In the process cache the resource.
     *
     * @param rowMap
     * @return The schema corresponding to the row.
     */
    private String assembleSchema(RowMap rowMap) {
        try {
            JSONObject schemaInfo = getResourceWithCache(getResourceKey(rowMap.getTable()));

            // get the schema. the structure contains both schema and metadata
            JSONObject schema = schemaInfo.getJSONObject(this.LOOKUP_SCHEMA);

            // if this schema contains a nested schema - translating a column's string data into a json object -
            // find the nested schema and add it to the parent
            if (getMetadata(schemaInfo).getBoolean(this.LOOKUP_HAS_NESTED_SCHEMA)) {
                JSONObject metadata = getMetadata(schemaInfo);
                String uri = metadata.getString(this.LOOKUP_CHILD_SCHEMA)
                        + rowMap.getData(metadata.getString(this.LOOKUP_KEY))
                        + this.JSON_SUFFIX;
                JSONObject childSchema = getResourceWithCache(uri).getJSONObject(this.LOOKUP_SCHEMA);
                String dataColumn = metadata.getString(this.LOOKUP_DATA);

                // find the dataColumn field and store the child schema in it
                for (Object field : schema.getJSONArray(this.FIELDS)) {
                    JSONObject f = (JSONObject) field;
                    if (f.getString(this.NAME).equals(dataColumn)) {
                        f.put(this.TYPE, childSchema);
                        break;
                    }
                }
            }
            return schema.toString();
        } catch (JSONException e) {
            throw new RuntimeException("No schema information for table " + rowMap.getTable() + " - " + e.toString());
        }
    }

    /**
     * Lookup the schemaInfo's metadata.
     *
     * @param schemaInfo
     * @return the metadata JSONObject
     */
    private JSONObject getMetadata(JSONObject schemaInfo) {
        return schemaInfo.getJSONObject(this.LOOKUP_METADATA);
    }

    /**
     * Lookup a uri in our cache. If it doesn't exist load the resource, cache it, then return.
     *
     * @param uri
     * @return the JSONObject corresponding to the uri
     */
    private JSONObject getResourceWithCache(String uri) {
        JSONObject schemaInfo;
        if (this.resourceCache.containsKey(uri)){
            schemaInfo = this.resourceCache.get(uri);
        } else {
            schemaInfo = loadResource(uri);
            this.resourceCache.put(uri, schemaInfo);
        }

        return schemaInfo;
    }

    /**
     * Why would we cache the schema? See the lengthy description where schemaCache is declared.
     * In short, the Avro Schema Registry client chooses an IdentityHasHMap for performance reasons.
     *
     * @param schemaString The schema corresponding to the current row of data.
     * @return The cached schema object.
     */
    private Schema getSchemaFromCache(String schemaString) {
        Schema schema;
        if (schemaCache.containsKey(schemaString)) {
            schema = this.schemaCache.get(schemaString);
        } else {
            schema = new Schema.Parser().parse(schemaString);
            schemaCache.put(schemaString, schema);
        }
        return schema;
    }

    /**
     * Lookup the key for a given table.
     *
     * @param tableName
     * @return the key for a given table
     */
    private String getResourceKey(String tableName) {
        return this.schemaMappingURI + tableName + this.JSON_SUFFIX;
    }

    /**
     * Given a schema and key lookup all valid types for the key
     *
     * @param schema
     * @param dataKey
     * @return An ArrayList containing the valid types for the key.
     */
    private ArrayList<String> getTypesForSchema(Schema schema, String dataKey) {
        ArrayList<String> validTypes = new ArrayList<String>();
        Schema typeSchema = ((Schema.Field) schema.getField(dataKey)).schema();

        // if we have 1 type for this field add it.
        // if we have multiple types. populate them all.
        if (!typeSchema.getType().getName().equals("union")) {
            validTypes.add(typeSchema.getType().getName());
        } else {

            List<Schema> types = ((Schema.Field) schema.getField(dataKey)).schema().getTypes();
            Schema[] schemas = types.toArray(new Schema[types.size()]);

            // we only know how to deal with unions of null and one specific type
            // ok: ["null", "int"]
            // not ok: ["int", "string"]
            if (schemas.length > 2 || (schemas.length == 2 && !schemas.toString().contains("null"))) {
                throw new RuntimeException(">2 types for key " + dataKey);
            }

            for (Schema s : schemas) {
                validTypes.add(s.getName());
            }
        }
        return validTypes;
    }

    /**
     * Create and populate a GenericRecord based on a schema and a row of data.
     *
     * @param schema The Schema object.
     * @param rowMap The row of data.
     * @return A GenericRecord based on the Schema and row data.
     */
    private GenericRecord populateSchemaFromRowMap(Schema schema, RowMap rowMap) {
        GenericRecord record = new GenericData.Record(schema);
        // TODO: why looking schemainfo up again?
        JSONObject schemaInfo = this.resourceCache.get(getResourceKey(rowMap.getTable()));

        String dataColumn = null;
        if (getMetadata(schemaInfo).getBoolean(this.LOOKUP_HAS_NESTED_SCHEMA)) {
            dataColumn = getMetadata(schemaInfo).getString(this.LOOKUP_DATA);
        }

        for (String dataKey : rowMap.getDataKeys()) {
            Object data = rowMap.getData(dataKey);
            // TODO: what other types need dealing with?
            // TODO/NOTABLE: when creating a schema if you specify multiple types for a field you get the field type with the data:
            // TODO/NOTABLE: {"name": "field_foo", "type":["null", "string"]} -> {field_foo: {string: <value>}}
            // TODO/NOTABLE: if you specify one type for a field you get just the data:
            // TODO/NOTABLE: {"name": "field_foo", "type": "string"} -> {field_foo: <value>}
            // TODO/NOTABLE: this was found using the kafka-avro-console-consume. YMMV.
            ArrayList<String> validTypes = getTypesForSchema(schema, dataKey);

            if (data == null) {
                if (!validTypes.contains(TYPE_NULL)) {
                    throw new RuntimeException("Invalid null value found in field " + schema.getField(dataKey).name());
                }
                record.put(dataKey, null);
            } else if (validTypes.contains((TYPE_STRING))) {
                record.put(dataKey, data.toString());
            } else if (validTypes.contains((TYPE_INT))) {
                record.put(dataKey, Integer.parseInt(data.toString()));
            } else if (validTypes.contains((TYPE_BOOLEAN))) {
                record.put(dataKey, Boolean.parseBoolean(data.toString()));
            } else if (validTypes.contains(TYPE_DOUBLE)) {
                record.put(dataKey, Double.parseDouble(data.toString()));
            } else if (validTypes.contains((TYPE_FLOAT))) {
                record.put(dataKey, Float.parseFloat(data.toString()));
            } else if (validTypes.contains((TYPE_LONG))) {
                record.put(dataKey, Long.parseLong(data.toString()));
            } else if (dataColumn != null && dataKey.equals(dataColumn)) {
                // we have a column that contains a child record and we need to apply a schema to it
                // get the child schema for the dataColumn
                Schema childSchema = ((Schema) record.getSchema()).getField(dataColumn).schema();
                // populate the data.
                GenericRecord childRecord = populateSchemaFromJson(childSchema, new JSONObject(data.toString()));
                // finally add the data to the record
                record.put(dataKey, childRecord);
            } else {
                throw new RuntimeException("Unknown types in populateSchemaFromRowMap(): " + validTypes.toString());
            }
        }
        return record;
    }

    /**
     * Create and populate a GenericRecord from a Schema and a Json object.
     *
     * @param schema The Schema object.
     * @param data the Json object.
     * @return A GenericRecord based on the Schema and the Json object.
     */
    private GenericRecord populateSchemaFromJson(Schema schema, JSONObject data) {
        GenericRecord record = new GenericData.Record(schema);
        for (String key : data.keySet()) {
            // if this key contains a record call populateSchemaFromJson again to populate it.
            // otherwise just add the data
            if (schema.getField(key).schema().getType().getName().equals("record")) {
                record.put(key, populateSchemaFromJson(schema.getField(key).schema(), (JSONObject) data.get(key)));
            } else {
                Object obj = data.get(key);
                if (obj instanceof Double) {
                    record.put(key, ((Double) obj).doubleValue());
                } else if (obj instanceof Float) {
                    record.put(key, ((Float) obj).floatValue());
                } else if (obj instanceof Long) {
                    record.put(key, ((Long) obj).longValue());
                } else if (obj instanceof Integer) {
                    record.put(key, ((Integer) obj).intValue());
                } else if (obj instanceof String) {
                    record.put(key, obj.toString());
                } else {
                    throw new RuntimeException("Unknown type mapping from " + obj.getClass());
                }
            }
        }
        return record;
    }

    protected Integer getNumPartitions(String topic) {
        try {
            return this.kafka.partitionsFor(topic).size(); //returns 1 for new topics
        } catch (KafkaException e) {
            LOGGER.error("Topic '" + topic + "' name does not exist. Exception: " + e.getLocalizedMessage());
            throw e;
        }
    }

    @Override
    public void push(RowMap r) throws Exception {
        String key = r.pkToJson(keyFormat);
        String value = r.toJSON(outputConfig);

        if ( value == null ) { // heartbeat row or other row with suppressed output
            skipMessage(r);
            return;
        }

        ProducerRecord<String, GenericRecord> record;
        GenericRecord genericRecord;
        if (r instanceof DDLMap) {
            genericRecord = ddlSchemaRecord;
            genericRecord.put("value", value);
            record = new ProducerRecord<>(this.ddlTopic, this.ddlPartitioner.kafkaPartition(r, getNumPartitions(this.ddlTopic)), key, genericRecord);
        } else {
            // TODO: test against RI
            String schemaString = assembleSchema(r);
            Schema schema = getSchemaFromCache(schemaString);

            genericRecord = populateSchemaFromRowMap(schema, r);
            String topic = generateTopic(this.topic, r);
            record = new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, getNumPartitions(topic)), key, genericRecord);
        }

        KafkaCallback callback = new KafkaCallback(inflightMessages, r.getPosition(), r.isTXCommit(), this.context, key, genericRecord);

        if ( r.isTXCommit() )
            inflightMessages.addMessage(r.getPosition());


		/* if debug logging isn't enabled, release the reference to `value`, which can ease memory pressure somewhat */
        if ( !KafkaCallback.LOGGER.isDebugEnabled() )
            value = null;

        kafka.send(record, callback);
    }
}
