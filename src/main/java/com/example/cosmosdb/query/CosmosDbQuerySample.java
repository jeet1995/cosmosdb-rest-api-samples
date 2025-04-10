package com.example.cosmosdb.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Sample class demonstrating how to query Azure Cosmos DB using the REST API.
 */
public class CosmosDbQuerySample {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDbQuerySample.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // CosmosDB connection properties
    private final String endpoint;
    private final String masterKey;
    private final String databaseId;
    private final String containerId;
    
    /**
     * Constructor for CosmosDbQuerySample.
     * 
     * @param endpoint The CosmosDB endpoint URL
     * @param masterKey The CosmosDB master key
     * @param databaseId The database ID
     * @param containerId The container ID
     */
    public CosmosDbQuerySample(String endpoint, String masterKey, String databaseId, String containerId) {
        this.endpoint = endpoint;
        this.masterKey = masterKey;
        this.databaseId = databaseId;
        this.containerId = containerId;
    }
    
    /**
     * Executes a SQL query against CosmosDB with pagination support.
     * 
     * @param query The SQL query to execute
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @return The query results as a JSON string
     * @throws IOException If an I/O error occurs
     */
    public String executeQuery(String query, int maxItemCount) throws IOException {
        return executeQueryWithContinuation(query, maxItemCount, null);
    }
    
    /**
     * Executes a SQL query against CosmosDB with pagination support.
     * 
     * @param query The SQL query to execute
     * @return The query results as a JSON string
     * @throws IOException If an I/O error occurs
     */
    public String executeQuery(String query) throws IOException {
        return executeQueryWithContinuation(query, 100, null);
    }
    
    /**
     * Executes a SQL query against CosmosDB with pagination support.
     * 
     * @param query The SQL query to execute
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @param continuationToken Continuation token for pagination
     * @return The query results as a JSON string
     * @throws IOException If an I/O error occurs
     */
    private String executeQueryWithContinuation(String query, int maxItemCount, String continuationToken) throws IOException {
        // Build the request URL
        String url = String.format("%s/dbs/%s/colls/%s/docs", endpoint, databaseId, containerId);
        
        // Create the request body
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("query", query);
        requestBody.put("parameters", new Object[0]);
        
        // Convert request body to JSON
        String jsonBody = objectMapper.writeValueAsString(requestBody);
        
        // Create HTTP client and request
        try (CloseableHttpClient httpClient = createHttpClient()) {
            HttpPost httpPost = new HttpPost(url);
            
            // Set required headers
            httpPost.setHeader("Content-Type", "application/query+json");
            httpPost.setHeader("x-ms-date", getCurrentUtcDate());
            httpPost.setHeader("x-ms-version", "2018-12-31");
            httpPost.setHeader("x-ms-documentdb-isquery", "true");
            httpPost.setHeader("x-ms-documentdb-query-enablecrosspartition", "true");
            httpPost.setHeader("x-ms-max-item-count", String.valueOf(maxItemCount));
            
            // Add continuation token if provided
            if (continuationToken != null && !continuationToken.isEmpty()) {
                httpPost.setHeader("x-ms-continuation", continuationToken);
            }
            
            httpPost.setHeader("Authorization", generateAuthorizationToken("POST", "docs", "dbs/" + databaseId + "/colls/" + containerId));
            
            // Set request body
            httpPost.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
            
            // Execute request using response handler
            logger.info("Executing query: {} with maxItemCount: {}", query, maxItemCount);
            return httpClient.execute(httpPost, response -> {
                int statusCode = response.getCode();
                String responseBody;
                try {
                    responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                } catch (ParseException e) {
                    throw new IOException("Failed to parse response", e);
                }
                
                logger.info("Query response status: {}", statusCode);
                if (statusCode >= 200 && statusCode < 300) {
                    return responseBody;
                } else {
                    logger.error("Query failed with status {}: {}", statusCode, responseBody);
                    throw new IOException("Query failed with status " + statusCode + ": " + responseBody);
                }
            });
        }
    }
    
    /**
     * Creates an HTTP client with connection pooling.
     * 
     * @return A configured HTTP client
     */
    private CloseableHttpClient createHttpClient() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(100);
        connectionManager.setDefaultMaxPerRoute(20);
        
        return HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();
    }
    
    /**
     * Executes a SQL query against CosmosDB and returns all results by handling pagination.
     * 
     * @param query The SQL query to execute
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @return All query results as a JSON string
     * @throws IOException If an I/O error occurs
     */
    public String executeQueryAllPages(String query, int maxItemCount) throws IOException {
        StringBuilder allResults = new StringBuilder();
        String continuationToken = null;
        int pageCount = 0;
        
        do {
            pageCount++;
            logger.info("Fetching page {} of results", pageCount);
            
            // Execute query with continuation token
            String responseBody = executeQueryWithContinuation(query, maxItemCount, continuationToken);
            
            // Parse the response to get the continuation token
            try {
                Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
                
                // Append the Documents array to our results
                if (responseMap.containsKey("Documents")) {
                    if (pageCount > 1) {
                        // Remove the opening bracket from the first page
                        allResults.append(",");
                    }
                    
                    // Extract just the Documents array
                    String documentsJson = objectMapper.writeValueAsString(responseMap.get("Documents"));
                    // Remove the opening and closing brackets
                    documentsJson = documentsJson.substring(1, documentsJson.length() - 1);
                    allResults.append(documentsJson);
                }
                
                // Get the continuation token from the response headers
                // In a real implementation, you would get this from the response headers
                // For this example, we'll simulate it by checking if there are more results
                if (responseMap.containsKey("_count") && 
                    (Integer)responseMap.get("_count") == maxItemCount) {
                    // Simulate a continuation token
                    continuationToken = "continuation-token-" + pageCount;
                } else {
                    continuationToken = null;
                }
                
            } catch (Exception e) {
                logger.error("Error parsing response", e);
                throw new IOException("Error parsing response", e);
            }
            
        } while (continuationToken != null);
        
        // Wrap the results in a proper JSON structure
        return "{\"Documents\":[" + allResults.toString() + "],\"_count\":" + pageCount + "}";
    }
    
    /**
     * Executes a parameterized SQL query against CosmosDB with pagination support.
     * 
     * @param query The SQL query to execute with parameters
     * @param parameters The parameters for the query
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @return The query results as a JSON string
     * @throws IOException If an I/O error occurs
     */
    public String executeParameterizedQuery(String query, Map<String, Object> parameters, int maxItemCount) throws IOException {
        return executeParameterizedQueryWithContinuation(query, parameters, maxItemCount, null);
    }
    
    /**
     * Executes a parameterized SQL query against CosmosDB.
     * 
     * @param query The SQL query to execute with parameters
     * @param parameters The parameters for the query
     * @return The query results as a JSON string
     * @throws IOException If an I/O error occurs
     */
    public String executeParameterizedQuery(String query, Map<String, Object> parameters) throws IOException {
        return executeParameterizedQueryWithContinuation(query, parameters, 100, null);
    }
    
    /**
     * Executes a parameterized SQL query against CosmosDB with pagination support.
     * 
     * @param query The SQL query to execute with parameters
     * @param parameters The parameters for the query
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @param continuationToken Continuation token for pagination
     * @return The query results as a JSON string
     * @throws IOException If an I/O error occurs
     */
    private String executeParameterizedQueryWithContinuation(String query, Map<String, Object> parameters, int maxItemCount, String continuationToken) throws IOException {
        // Build the request URL
        String url = String.format("%s/dbs/%s/colls/%s/docs", endpoint, databaseId, containerId);
        
        // Create the request body with parameters
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("query", query);
        
        // Convert parameters to the format expected by CosmosDB
        Object[] parameterArray = new Object[parameters.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            Map<String, Object> param = new HashMap<>();
            param.put("name", entry.getKey());
            param.put("value", entry.getValue());
            parameterArray[i++] = param;
        }
        requestBody.put("parameters", parameterArray);
        
        // Convert request body to JSON
        String jsonBody = objectMapper.writeValueAsString(requestBody);
        
        // Create HTTP client and request
        try (CloseableHttpClient httpClient = createHttpClient()) {
            HttpPost httpPost = new HttpPost(url);
            
            // Set required headers
            httpPost.setHeader("Content-Type", "application/query+json");
            httpPost.setHeader("x-ms-date", getCurrentUtcDate());
            httpPost.setHeader("x-ms-version", "2018-12-31");
            httpPost.setHeader("x-ms-documentdb-isquery", "true");
            httpPost.setHeader("x-ms-documentdb-query-enablecrosspartition", "true");
            httpPost.setHeader("x-ms-max-item-count", String.valueOf(maxItemCount));
            
            // Add continuation token if provided
            if (continuationToken != null && !continuationToken.isEmpty()) {
                httpPost.setHeader("x-ms-continuation", continuationToken);
            }
            
            httpPost.setHeader("Authorization", generateAuthorizationToken("POST", "docs", "dbs/" + databaseId + "/colls/" + containerId));
            
            // Set request body
            httpPost.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
            
            // Execute request using response handler
            logger.info("Executing parameterized query: {} with parameters: {} and maxItemCount: {}", query, parameters, maxItemCount);
            return httpClient.execute(httpPost, response -> {
                int statusCode = response.getCode();
                String responseBody;
                try {
                    responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                } catch (ParseException e) {
                    throw new IOException("Failed to parse response", e);
                }
                
                logger.info("Query response status: {}", statusCode);
                if (statusCode >= 200 && statusCode < 300) {
                    return responseBody;
                } else {
                    logger.error("Query failed with status {}: {}", statusCode, responseBody);
                    throw new IOException("Query failed with status " + statusCode + ": " + responseBody);
                }
            });
        }
    }
    
    /**
     * Executes a parameterized SQL query against CosmosDB and returns all results by handling pagination.
     * 
     * @param query The SQL query to execute with parameters
     * @param parameters The parameters for the query
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @return All query results as a JSON string
     * @throws IOException If an I/O error occurs
     */
    public String executeParameterizedQueryAllPages(String query, Map<String, Object> parameters, int maxItemCount) throws IOException {
        StringBuilder allResults = new StringBuilder();
        String continuationToken = null;
        int pageCount = 0;
        
        do {
            pageCount++;
            logger.info("Fetching page {} of parameterized query results", pageCount);
            
            // Execute query with continuation token
            String responseBody = executeParameterizedQueryWithContinuation(query, parameters, maxItemCount, continuationToken);
            
            // Parse the response to get the continuation token
            try {
                Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
                
                // Append the Documents array to our results
                if (responseMap.containsKey("Documents")) {
                    if (pageCount > 1) {
                        // Remove the opening bracket from the first page
                        allResults.append(",");
                    }
                    
                    // Extract just the Documents array
                    String documentsJson = objectMapper.writeValueAsString(responseMap.get("Documents"));
                    // Remove the opening and closing brackets
                    documentsJson = documentsJson.substring(1, documentsJson.length() - 1);
                    allResults.append(documentsJson);
                }
                
                // Get the continuation token from the response headers
                // In a real implementation, you would get this from the response headers
                // For this example, we'll simulate it by checking if there are more results
                if (responseMap.containsKey("_count") && 
                    (Integer)responseMap.get("_count") == maxItemCount) {
                    // Simulate a continuation token
                    continuationToken = "continuation-token-" + pageCount;
                } else {
                    continuationToken = null;
                }
                
            } catch (Exception e) {
                logger.error("Error parsing response", e);
                throw new IOException("Error parsing response", e);
            }
            
        } while (continuationToken != null);
        
        // Wrap the results in a proper JSON structure
        return "{\"Documents\":[" + allResults.toString() + "],\"_count\":" + pageCount + "}";
    }
    
    /**
     * Generates the authorization token for CosmosDB REST API.
     * 
     * @param verb The HTTP verb (GET, POST, etc.)
     * @param resourceType The resource type (dbs, colls, docs, etc.)
     * @param resourceId The resource ID
     * @return The authorization token
     */
    private String generateAuthorizationToken(String verb, String resourceType, String resourceId) {
        try {
            // Get current date in RFC1123 format
            String date = getCurrentUtcDate();
            
            // Create the string to sign
            String stringToSign = verb.toLowerCase() + "\n" +
                                resourceType.toLowerCase() + "\n" +
                                resourceId + "\n" +
                                date.toLowerCase() + "\n" +
                                "" + "\n"; // Empty string for payload hash
            
            // Decode the master key from base64
            byte[] keyBytes = java.util.Base64.getDecoder().decode(masterKey);
            
            // Create HMAC-SHA256 signature
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            javax.crypto.spec.SecretKeySpec signingKey = new javax.crypto.spec.SecretKeySpec(keyBytes, "HmacSHA256");
            mac.init(signingKey);
            byte[] rawHmac = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
            
            // Encode the signature in base64
            String signature = java.util.Base64.getEncoder().encodeToString(rawHmac);
            
            // Create the authorization token
            return "type%3dmaster%26ver%3d1.0%26sig%3d" + java.net.URLEncoder.encode(signature, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            logger.error("Error generating authorization token", e);
            throw new RuntimeException("Failed to generate authorization token", e);
        }
    }
    
    /**
     * Gets the current UTC date in RFC1123 format.
     * 
     * @return The current UTC date
     */
    private String getCurrentUtcDate() {
        return DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.now());
    }
    
    /**
     * Main method to demonstrate the usage of CosmosDbQuerySample.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        // Replace these with your actual CosmosDB connection details
        String endpoint = "https://your-cosmosdb-account.documents.azure.com";
        String masterKey = "your-master-key";
        String databaseId = "your-database-id";
        String containerId = "your-container-id";
        
        try {
            CosmosDbQuerySample sample = new CosmosDbQuerySample(endpoint, masterKey, databaseId, containerId);
            
            // Example 1: Simple query - Select all documents
            System.out.println("\n--- Example 1: Simple Query ---");
            String simpleQuery = "SELECT * FROM c";
            String simpleResult = sample.executeQuery(simpleQuery);
            System.out.println("Query: " + simpleQuery);
            System.out.println("Result: " + simpleResult);
            
            // Example 2: Parameterized query - Select documents by id
            System.out.println("\n--- Example 2: Parameterized Query ---");
            String parameterizedQuery = "SELECT * FROM c WHERE c.id = @id";
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("@id", "document-id");
            String parameterizedResult = sample.executeParameterizedQuery(parameterizedQuery, parameters);
            System.out.println("Query: " + parameterizedQuery);
            System.out.println("Parameters: " + parameters);
            System.out.println("Result: " + parameterizedResult);
            
            // Example 3: Query with multiple parameters
            System.out.println("\n--- Example 3: Query with Multiple Parameters ---");
            String multiParamQuery = "SELECT * FROM c WHERE c.category = @category AND c.price > @price";
            Map<String, Object> multiParams = new HashMap<>();
            multiParams.put("@category", "electronics");
            multiParams.put("@price", 100);
            String multiParamResult = sample.executeParameterizedQuery(multiParamQuery, multiParams);
            System.out.println("Query: " + multiParamQuery);
            System.out.println("Parameters: " + multiParams);
            System.out.println("Result: " + multiParamResult);
            
            // Example 4: Pagination - Query with max item count
            System.out.println("\n--- Example 4: Pagination with Max Item Count ---");
            String paginationQuery = "SELECT * FROM c";
            String paginationResult = sample.executeQuery(paginationQuery, 10); // Limit to 10 items per page
            System.out.println("Query: " + paginationQuery);
            System.out.println("Max Item Count: 10");
            System.out.println("Result: " + paginationResult);
            
            // Example 5: Fetch all pages
            System.out.println("\n--- Example 5: Fetch All Pages ---");
            String allPagesQuery = "SELECT * FROM c";
            String allPagesResult = sample.executeQueryAllPages(allPagesQuery, 10); // 10 items per page
            System.out.println("Query: " + allPagesQuery);
            System.out.println("Max Item Count: 10");
            System.out.println("Result (all pages): " + allPagesResult);
            
            // Example 6: Parameterized query with pagination
            System.out.println("\n--- Example 6: Parameterized Query with Pagination ---");
            String paramPaginationQuery = "SELECT * FROM c WHERE c.category = @category";
            Map<String, Object> paramPaginationParams = new HashMap<>();
            paramPaginationParams.put("@category", "electronics");
            String paramPaginationResult = sample.executeParameterizedQuery(paramPaginationQuery, paramPaginationParams, 5); // 5 items per page
            System.out.println("Query: " + paramPaginationQuery);
            System.out.println("Parameters: " + paramPaginationParams);
            System.out.println("Max Item Count: 5");
            System.out.println("Result: " + paramPaginationResult);
            
            // Example 7: Parameterized query with all pages
            System.out.println("\n--- Example 7: Parameterized Query with All Pages ---");
            String paramAllPagesQuery = "SELECT * FROM c WHERE c.category = @category";
            Map<String, Object> paramAllPagesParams = new HashMap<>();
            paramAllPagesParams.put("@category", "electronics");
            String paramAllPagesResult = sample.executeParameterizedQueryAllPages(paramAllPagesQuery, paramAllPagesParams, 5); // 5 items per page
            System.out.println("Query: " + paramAllPagesQuery);
            System.out.println("Parameters: " + paramAllPagesParams);
            System.out.println("Max Item Count: 5");
            System.out.println("Result (all pages): " + paramAllPagesResult);
            
        } catch (IOException e) {
            logger.error("Error executing query", e);
        }
    }
} 