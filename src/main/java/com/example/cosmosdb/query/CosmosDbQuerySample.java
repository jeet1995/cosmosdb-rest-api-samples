package com.example.cosmosdb.query;

import com.azure.cosmos.implementation.TestConfigurations;
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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Sample class demonstrating how to query Azure Cosmos DB using the REST API.
 */
public class CosmosDbQuerySample {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDbQuerySample.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String URL_FORMAT = "%s/dbs/%s/colls/%s/docs";

    // NOTE DateTimeFormatter.RFC_1123_DATE_TIME cannot be used.
    // because cosmos db rfc1123 validation requires two digits for day.
    // so Thu, 04 Jan 2018 00:30:37 GMT is accepted by the cosmos db service,
    // but Thu, 4 Jan 2018 00:30:37 GMT is not.
    // Therefore, we need a custom date time formatter.
    private static final DateTimeFormatter RFC_1123_DATE_TIME = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

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
            httpPost.setHeader("x-ms-version", "2018-12-31");
            httpPost.setHeader("x-ms-date", getCurrentUtcDate());
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
     * Gets the current UTC date in RFC1123 format.
     *
     * @return The current UTC date
     */
    private String getCurrentUtcDate() {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("GMT"));
        return RFC_1123_DATE_TIME.format(now);
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
    }
    
    /**
     * Main method to demonstrate the usage of CosmosDbQuerySample.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        // Replace these with your actual CosmosDB connection details
        String endpoint = TestConfigurations.HOST;
        String masterKey = TestConfigurations.MASTER_KEY;
        String databaseId = "CRI";
        String containerId = "AdobeQuery";
        
        try {
            CosmosDbQuerySample sample = new CosmosDbQuerySample(endpoint, masterKey, databaseId, containerId);

            // Example: Count all documents with pagination
            System.out.println("\n--- Example 5: Query with where clause ---");
            String queryWithWhereClause = "SELECT * FROM c WHERE c.expectedProcessTime >= '2019-06-01T00:00' AND c.expectedProcessTime <= '2039-06-01T00:00'";
            int totalItemCount = sample.executeQueryAllPages(queryWithWhereClause, 10); // 10 items per page
            System.out.println("Query: " + queryWithWhereClause);
            System.out.println("Max Item Count: 10");
            System.out.println("Total documents: " + totalItemCount);
            
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