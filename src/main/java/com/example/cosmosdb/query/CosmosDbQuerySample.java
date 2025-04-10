package com.example.cosmosdb.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Sample class demonstrating how to query Azure Cosmos DB using the REST API.
 */
public class CosmosDbQuerySample {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDbQuerySample.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String URL_FORMAT = "https://%s.documents.azure.com/dbs/%s/colls/%s/docs";
    
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
     * @param continuationToken Continuation token for pagination
     * @return The query results as a Page object containing the response body and continuation token
     * @throws IOException If an I/O error occurs
     */
    private Page executeQueryWithContinuation(String query, int maxItemCount, String continuationToken) throws IOException {
        // Generate a correlated activity ID for this query
        String correlatedActivityId = UUID.randomUUID().toString();
        logger.info("Generated correlated activity ID: {}", correlatedActivityId);
        
        return executeQueryWithContinuation(query, maxItemCount, continuationToken, correlatedActivityId);
    }
    
    /**
     * Executes a SQL query against CosmosDB with pagination support.
     * 
     * @param query The SQL query to execute
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @param continuationToken Continuation token for pagination
     * @param correlatedActivityId The correlated activity ID for tracking related requests
     * @return The query results as a Page object containing the response body and continuation token
     * @throws IOException If an I/O error occurs
     */
    private Page executeQueryWithContinuation(String query, int maxItemCount, String continuationToken, String correlatedActivityId) throws IOException {
        // Build the request URL
        String url = String.format(URL_FORMAT, endpoint, databaseId, containerId);
        
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
            
            // Add correlated activity ID header
            httpPost.setHeader("x-ms-cosmos-correlated-activityid", correlatedActivityId);
            
            // Set continuation token size limit to 2KB
            httpPost.setHeader("x-ms-documentdb-responsecontinuationtokenlimitinkb", "2");
            
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
                    // Extract continuation token from response headers
                    String nextContinuationToken = null;
                    Header continuationHeader = response.getHeader("x-ms-continuation");
                    if (continuationHeader != null) {
                        nextContinuationToken = continuationHeader.getValue();
                        logger.info("Continuation token found: {}", nextContinuationToken);
                    }
                    
                    // Parse the response to extract documents
                    List<Map<String, Object>> documents = null;
                    try {
                        Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
                        if (responseMap.containsKey("Documents")) {
                            documents = (List<Map<String, Object>>) responseMap.get("Documents");
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to parse documents from response", e);
                    }
                    
                    return new Page(responseBody, documents, nextContinuationToken);
                } else {
                    logger.error("Query failed with status {}: {}", statusCode, responseBody);
                    throw new IOException("Query failed with status " + statusCode + ": " + responseBody);
                }
            });
        }
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
        // Generate a correlated activity ID for this query
        String correlatedActivityId = UUID.randomUUID().toString();
        logger.info("Generated correlated activity ID: {}", correlatedActivityId);
        
        Page page = executeQueryWithContinuation(query, maxItemCount, null, correlatedActivityId);
        return page.getResponseBody();
    }
    
    /**
     * Executes a SQL query against CosmosDB with pagination support.
     * 
     * @param query The SQL query to execute
     * @return The query results as a JSON string
     * @throws IOException If an I/O error occurs
     */
    public String executeQuery(String query) throws IOException {
        // Generate a correlated activity ID for this query
        String correlatedActivityId = UUID.randomUUID().toString();
        logger.info("Generated correlated activity ID: {}", correlatedActivityId);
        
        Page page = executeQueryWithContinuation(query, 100, null, correlatedActivityId);
        return page.getResponseBody();
    }
    
    /**
     * Executes a SQL query against CosmosDB and returns all results by handling pagination.
     * 
     * @param query The SQL query to execute
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @return The total count of documents returned
     * @throws IOException If an I/O error occurs
     */
    public int executeQueryAllPages(String query, int maxItemCount) throws IOException {
        int totalDocumentCount = 0;
        String continuationToken = null;
        int pageCount = 0;
        
        // Generate a correlated activity ID that will be used across all requests
        String correlatedActivityId = UUID.randomUUID().toString();
        logger.info("Generated correlated activity ID: {}", correlatedActivityId);
        
        do {
            pageCount++;
            logger.info("Fetching page {} of results", pageCount);
            
            // Execute query with continuation token
            Page page = executeQueryWithContinuation(query, maxItemCount, continuationToken, correlatedActivityId);
            continuationToken = page.getContinuationToken();
            
            // Count documents in this page
            if (page.getDocuments() != null) {
                int pageDocumentCount = page.getDocuments().size();
                totalDocumentCount += pageDocumentCount;
                logger.info("Page {} contains {} documents. Total so far: {}", pageCount, pageDocumentCount, totalDocumentCount);
            }
            
        } while (continuationToken != null);
        
        logger.info("Query completed. Total pages: {}, Total documents: {}", pageCount, totalDocumentCount);
        return totalDocumentCount;
    }
    
    /**
     * Executes a parameterized SQL query against CosmosDB with pagination support.
     * 
     * @param query The SQL query to execute with parameters
     * @param parameters The parameters for the query
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @param continuationToken Continuation token for pagination
     * @param correlatedActivityId The correlated activity ID for tracking related requests
     * @return The query results as a Page object containing the response body and continuation token
     * @throws IOException If an I/O error occurs
     */
    private Page executeParameterizedQueryWithContinuation(String query, Map<String, Object> parameters, int maxItemCount, String continuationToken, String correlatedActivityId) throws IOException {
        // Build the request URL
        String url = String.format(URL_FORMAT, endpoint, databaseId, containerId);
        
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
            
            // Add correlated activity ID header
            httpPost.setHeader("x-ms-cosmos-correlated-activityid", correlatedActivityId);
            
            // Set continuation token size limit to 2KB
            httpPost.setHeader("x-ms-documentdb-responsecontinuationtokenlimitinkb", "2");
            
            // Add continuation token if provided
            if (continuationToken != null && !continuationToken.isEmpty()) {
                httpPost.setHeader("x-ms-continuation", continuationToken);
            }
            
            httpPost.setHeader("Authorization", generateAuthorizationToken("POST", "docs", "dbs/" + databaseId + "/colls/" + containerId));
            
            // Set request body
            httpPost.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
            
            // Execute request using response handler
            logger.info("Executing parameterized query: {} with parameters: {} and maxItemCount: {} and correlated activity ID: {}", query, parameters, maxItemCount, correlatedActivityId);
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
                    // Extract continuation token from response headers
                    String nextContinuationToken = null;
                    Header continuationHeader = response.getHeader("x-ms-continuation");
                    if (continuationHeader != null) {
                        nextContinuationToken = continuationHeader.getValue();
                        logger.info("Continuation token found: {}", nextContinuationToken);
                    }
                    
                    // Parse the response to extract documents
                    List<Map<String, Object>> documents = null;
                    try {
                        Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
                        if (responseMap.containsKey("Documents")) {
                            documents = (List<Map<String, Object>>) responseMap.get("Documents");
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to parse documents from response", e);
                    }
                    
                    return new Page(responseBody, documents, nextContinuationToken);
                } else {
                    logger.error("Query failed with status {}: {}", statusCode, responseBody);
                    throw new IOException("Query failed with status " + statusCode + ": " + responseBody);
                }
            });
        }
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
        // Generate a correlated activity ID for this query
        String correlatedActivityId = UUID.randomUUID().toString();
        logger.info("Generated correlated activity ID: {}", correlatedActivityId);
        
        Page page = executeParameterizedQueryWithContinuation(query, parameters, maxItemCount, null, correlatedActivityId);
        return page.getResponseBody();
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
        // Generate a correlated activity ID for this query
        String correlatedActivityId = UUID.randomUUID().toString();
        logger.info("Generated correlated activity ID: {}", correlatedActivityId);
        
        Page page = executeParameterizedQueryWithContinuation(query, parameters, 100, null, correlatedActivityId);
        return page.getResponseBody();
    }
    
    /**
     * Executes a parameterized SQL query against CosmosDB and returns all results by handling pagination.
     * 
     * @param query The SQL query to execute with parameters
     * @param parameters The parameters for the query
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @return The total count of documents returned
     * @throws IOException If an I/O error occurs
     */
    public int executeParameterizedQueryAllPages(String query, Map<String, Object> parameters, int maxItemCount) throws IOException {
        int totalDocumentCount = 0;
        String continuationToken = null;
        int pageCount = 0;
        
        // Generate a correlated activity ID that will be used across all requests
        String correlatedActivityId = UUID.randomUUID().toString();
        logger.info("Generated correlated activity ID: {}", correlatedActivityId);
        
        do {
            pageCount++;
            logger.info("Fetching page {} of parameterized query results", pageCount);
            
            // Execute query with continuation token
            Page page = executeParameterizedQueryWithContinuation(query, parameters, maxItemCount, continuationToken, correlatedActivityId);
            continuationToken = page.getContinuationToken();
            
            // Count documents in this page
            if (page.getDocuments() != null) {
                int pageDocumentCount = page.getDocuments().size();
                totalDocumentCount += pageDocumentCount;
                logger.info("Page {} contains {} documents. Total so far: {}", pageCount, pageDocumentCount, totalDocumentCount);
            }
            
        } while (continuationToken != null);
        
        logger.info("Parameterized query completed. Total pages: {}, Total documents: {}", pageCount, totalDocumentCount);
        return totalDocumentCount;
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
     * Class to represent a page of query results.
     * Contains the raw response body, parsed documents, and continuation token.
     */
    public static class Page {
        private final String responseBody;
        private final List<Map<String, Object>> documents;
        private final String continuationToken;
        
        /**
         * Constructor for Page.
         * 
         * @param responseBody The raw response body as a JSON string
         * @param documents The parsed documents from the response
         * @param continuationToken The continuation token for the next page
         */
        public Page(String responseBody, List<Map<String, Object>> documents, String continuationToken) {
            this.responseBody = responseBody;
            this.documents = documents;
            this.continuationToken = continuationToken;
        }
        
        /**
         * Gets the raw response body.
         * 
         * @return The response body as a JSON string
         */
        public String getResponseBody() {
            return responseBody;
        }
        
        /**
         * Gets the parsed documents from the response.
         * 
         * @return The list of documents
         */
        public List<Map<String, Object>> getDocuments() {
            return documents;
        }
        
        /**
         * Gets the continuation token for the next page.
         * 
         * @return The continuation token, or null if there are no more pages
         */
        public String getContinuationToken() {
            return continuationToken;
        }
        
        /**
         * Checks if there are more pages available.
         * 
         * @return true if there are more pages, false otherwise
         */
        public boolean hasMorePages() {
            return continuationToken != null && !continuationToken.isEmpty();
        }
    }
    
    /**
     * Main method to demonstrate the usage of CosmosDbQuerySample.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        // Replace these with your actual CosmosDB connection details
        String endpoint = "your-cosmosdb-account";
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
            
            // Example 5: Count all documents with pagination
            System.out.println("\n--- Example 5: Count All Documents ---");
            String allPagesQuery = "SELECT * FROM c";
            int totalDocuments = sample.executeQueryAllPages(allPagesQuery, 10); // 10 items per page
            System.out.println("Query: " + allPagesQuery);
            System.out.println("Max Item Count: 10");
            System.out.println("Total Documents: " + totalDocuments);
            
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
            
            // Example 7: Count documents with parameterized query
            System.out.println("\n--- Example 7: Count Documents with Parameterized Query ---");
            String paramAllPagesQuery = "SELECT * FROM c WHERE c.category = @category";
            Map<String, Object> paramAllPagesParams = new HashMap<>();
            paramAllPagesParams.put("@category", "electronics");
            int totalParamDocuments = sample.executeParameterizedQueryAllPages(paramAllPagesQuery, paramAllPagesParams, 5); // 5 items per page
            System.out.println("Query: " + paramAllPagesQuery);
            System.out.println("Parameters: " + paramAllPagesParams);
            System.out.println("Max Item Count: 5");
            System.out.println("Total Documents: " + totalParamDocuments);
            
        } catch (IOException e) {
            logger.error("Error executing query", e);
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
} 