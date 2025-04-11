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
import org.apache.hc.core5.http.ProtocolException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Sample class demonstrating how to query Azure Cosmos DB using the REST API.
 */
public class CosmosDbQuerySample {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String URL_FORMAT = "%s/dbs/%s/colls/%s/docs";
    
    // Singleton HTTP client
    private static volatile CloseableHttpClient httpClient;

    // NOTE DateTimeFormatter.RFC_1123_DATE_TIME cannot be used.
    // because cosmos db rfc1123 validation requires two digits for day.
    // so Thu, 04 Jan 2018 00:30:37 GMT is accepted by the cosmos db service,
    // but Thu, 4 Jan 2018 00:30:37 GMT is not.
    // Therefore, we need a custom date time formatter.
    private static final DateTimeFormatter RFC_1123_DATE_TIME = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

    // HTTP Headers
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String HEADER_MS_VERSION = "x-ms-version";
    private static final String HEADER_MS_DATE = "x-ms-date";
    private static final String HEADER_MS_DOCUMENTDB_IS_QUERY = "x-ms-documentdb-isquery";
    private static final String HEADER_MS_DOCUMENTDB_QUERY_ENABLE_CROSS_PARTITION = "x-ms-documentdb-query-enablecrosspartition";
    private static final String HEADER_MS_MAX_ITEM_COUNT = "x-ms-max-item-count";
    private static final String HEADER_MS_COSMOS_CORRELATED_ACTIVITY_ID = "x-ms-cosmos-correlated-activityid";
    private static final String HEADER_MS_DOCUMENTDB_RESPONSE_CONTINUATION_TOKEN_LIMIT_IN_KB = "x-ms-documentdb-responsecontinuationtokenlimitinkb";
    private static final String HEADER_MS_CONTINUATION = "x-ms-continuation";
    private static final String HEADER_AUTHORIZATION = "Authorization";

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
     * Gets the singleton HTTP client instance.
     * 
     * @return The HTTP client
     */
    private static CloseableHttpClient getHttpClient() {
        if (httpClient == null) {
            synchronized (CosmosDbQuerySample.class) {
                if (httpClient == null) {
                    httpClient = createHttpClient();
                }
            }
        }
        return httpClient;
    }

    /**
     * Executes a SQL query against CosmosDB with pagination support.
     * 
     * @param query The SQL query to execute
     * @param maxItemCount Maximum number of items to return per page
     * @param continuationToken Continuation token for pagination
     * @param correlatedActivityId The correlated activity ID for tracking related requests
     * @return The query results as a Page object containing the response body and continuation token
     * @throws IOException If an I/O error occurs
     * @throws ProtocolException If a protocol error occurs
     */
    private Page executeQueryWithContinuation(String query, int maxItemCount, String continuationToken, String correlatedActivityId) throws IOException, ProtocolException {
        // Build the request URL
        String url = String.format(URL_FORMAT, endpoint, databaseId, containerId);
        
        // Create the request body
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("query", query);
        requestBody.put("parameters", new Object[0]);
        
        // Convert request body to JSON
        String jsonBody = objectMapper.writeValueAsString(requestBody);
        
        // Create HTTP request
        HttpPost httpPost = new HttpPost(url);
        
        // Set required headers
        httpPost.setHeader(HEADER_CONTENT_TYPE, "application/query+json");
        httpPost.setHeader(HEADER_MS_VERSION, "2018-12-31");
        httpPost.setHeader(HEADER_MS_DATE, getCurrentUtcDate());
        httpPost.setHeader(HEADER_MS_DOCUMENTDB_IS_QUERY, "true");
        httpPost.setHeader(HEADER_MS_DOCUMENTDB_QUERY_ENABLE_CROSS_PARTITION, "true");
        httpPost.setHeader(HEADER_MS_MAX_ITEM_COUNT, String.valueOf(maxItemCount));
        
        // Add correlated activity ID header
        httpPost.setHeader(HEADER_MS_COSMOS_CORRELATED_ACTIVITY_ID, correlatedActivityId);
        
        // Set continuation token size limit to 2KB
        httpPost.setHeader(HEADER_MS_DOCUMENTDB_RESPONSE_CONTINUATION_TOKEN_LIMIT_IN_KB, "2");
        
        // Add continuation token if provided
        if (continuationToken != null && !continuationToken.isEmpty()) {
            httpPost.setHeader(HEADER_MS_CONTINUATION, continuationToken);
        }
        
        httpPost.setHeader(HEADER_AUTHORIZATION, generateAuthorizationToken("POST", "docs", "dbs/" + databaseId + "/colls/" + containerId));
        
        // Set request body
        httpPost.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
        
        // Execute request using response handler
        System.out.println("Executing query: " + query + " with maxItemCount: " + maxItemCount);

        // Parse the response to extract documents
        final List<Map<String, Object>> documents = new ArrayList<>();

        // Execute the request with the response handler
        return getHttpClient().execute(httpPost, response -> {
            int statusCode = response.getCode();
            String responseBody;
            try {
                responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            } catch (ParseException e) {
                throw new IOException("Failed to parse response", e);
            }
            
            System.out.println("Query response status: " + statusCode);
            if (statusCode >= 200 && statusCode < 300) {
                // Extract continuation token from response headers
                String nextContinuationToken = null;
                Header continuationHeader = response.getHeader(HEADER_MS_CONTINUATION);
                if (continuationHeader != null) {
                    nextContinuationToken = continuationHeader.getValue();
                    System.out.println("Continuation token found: " + nextContinuationToken);
                }

                try {
                    Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
                    if (responseMap.containsKey("Documents")) {

                        List<Map<String, Object>> documentsFromResponse = (List<Map<String, Object>>)responseMap.get("Documents");

                        documents.addAll(documentsFromResponse);
                    }
                } catch (Exception e) {
                    System.out.println("Failed to parse documents from response: " + e.getMessage());
                }
                
                return new Page(documents, nextContinuationToken);
            } else {
                System.out.println("Query failed with status " + statusCode + ": " + responseBody);
                throw new IOException("Query failed with status " + statusCode + ": " + responseBody);
            }
        });
    }

    /**
     * Executes a SQL query against CosmosDB with pagination support.
     *
     * @param query The SQL query to execute
     * @param pageSize Maximum number of items to return per page
     * @param currentContinuationToken Continuation token for pagination
     * @param correlatedActivityId The correlated activity ID for tracking related requests
     * @return The query results as a Page object containing the response body and continuation token
     * @throws IOException If an I/O error occurs
     * @throws ProtocolException If a protocol error occurs
     */
    private Page executeQueryWithContinuationAdhereToPageSize(String query, int pageSize, String currentContinuationToken, String correlatedActivityId) throws IOException, ProtocolException {
        // Build the request URL
        String url = String.format(URL_FORMAT, endpoint, databaseId, containerId);

        // Create the request body
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("query", query);
        requestBody.put("parameters", new Object[0]);

        // Convert request body to JSON
        String jsonBody = objectMapper.writeValueAsString(requestBody);

        // Create HTTP request
        HttpPost httpPost = new HttpPost(url);

        // Set required headers
        httpPost.setHeader(HEADER_CONTENT_TYPE, "application/query+json");
        httpPost.setHeader(HEADER_MS_VERSION, "2018-12-31");
        httpPost.setHeader(HEADER_MS_DATE, getCurrentUtcDate());
        httpPost.setHeader(HEADER_MS_DOCUMENTDB_IS_QUERY, "true");
        httpPost.setHeader(HEADER_MS_DOCUMENTDB_QUERY_ENABLE_CROSS_PARTITION, "true");

        // Add correlated activity ID header
        httpPost.setHeader(HEADER_MS_COSMOS_CORRELATED_ACTIVITY_ID, correlatedActivityId);

        // Set continuation token size limit to 2KB
        httpPost.setHeader(HEADER_MS_DOCUMENTDB_RESPONSE_CONTINUATION_TOKEN_LIMIT_IN_KB, "2");

        httpPost.setHeader(HEADER_AUTHORIZATION, generateAuthorizationToken("POST", "docs", "dbs/" + databaseId + "/colls/" + containerId));

        // Set request body
        httpPost.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));

        // Execute request using response handler
        System.out.println("Executing query: " + query + " with maxItemCount: " + pageSize);

        // Parse the response to extract documents
        final List<Map<String, Object>> pageSizeCompliantDocuments = new ArrayList<>();

        AtomicInteger remainingPageSize = new AtomicInteger(pageSize);
        AtomicReference<String> nextPageContinuation = new AtomicReference<>("INF");

        if (currentContinuationToken != null && !currentContinuationToken.isEmpty()) {
            // Add continuation token if provided
            nextPageContinuation.set(currentContinuationToken);
            httpPost.setHeader(HEADER_MS_CONTINUATION, currentContinuationToken);
        }

        while (nextPageContinuation.get() != null) {

            if (!nextPageContinuation.get().equals("INF")) {
                httpPost.setHeader(HEADER_MS_CONTINUATION, nextPageContinuation.get());
            }

            httpPost.setHeader(HEADER_MS_MAX_ITEM_COUNT, String.valueOf(remainingPageSize.get()));

            // Execute the request with the response handler
            Page currentMethodLocalPage = getHttpClient().execute(httpPost, response -> {
                int statusCode = response.getCode();
                String responseBody;
                try {
                    responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                } catch (ParseException e) {
                    throw new IOException("Failed to parse response", e);
                }

                System.out.println("Query response status: " + statusCode);
                if (statusCode >= 200 && statusCode < 300) {
                    // Extract continuation token from response headers
                    String nextContinuationToken = null;
                    nextPageContinuation.set(null);
                    Header continuationHeader = response.getHeader(HEADER_MS_CONTINUATION);

                    if (continuationHeader != null) {
                        nextContinuationToken = continuationHeader.getValue();
                        nextPageContinuation.set(nextContinuationToken);
                        System.out.println("Continuation token found: " + nextContinuationToken);
                    }

                    try {
                        Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
                        if (responseMap.containsKey("Documents")) {
                            List<Map<String, Object>> documentsFromResponse = (List<Map<String, Object>>) responseMap.get("Documents");
                            return new Page(documentsFromResponse, nextContinuationToken);
                        }
                    } catch (Exception e) {
                        System.out.println("Failed to parse documents from response: " + e.getMessage());
                    }
                } else {
                    System.out.println("Query failed with status " + statusCode + ": " + responseBody);
                    throw new IOException("Query failed with status " + statusCode + ": " + responseBody);
                }

                return new Page(null, null);
            });

            int seenIdx = 0;
            List<Map<String, Object>> currentPageDocuments = currentMethodLocalPage.documents;

            while (remainingPageSize.get() > 0 && currentPageDocuments != null) {

                if (seenIdx == currentPageDocuments.size()) {
                    break;
                }

                pageSizeCompliantDocuments.add(currentPageDocuments.get(seenIdx));
                remainingPageSize.decrementAndGet();
                seenIdx++;
            }

            if (remainingPageSize.get() == 0) {

                String nextContinuationToken = nextPageContinuation.get() != null
                        ? nextPageContinuation.get()
                        : "";

                return new Page(pageSizeCompliantDocuments, nextContinuationToken);
            }
        }

        return new Page(pageSizeCompliantDocuments, null);
    }

    /**
     * Executes a SQL query against CosmosDB and returns all results by handling pagination.
     * 
     * @param query The SQL query to execute
     * @param maxItemCount Maximum number of items to return per page (1-1000)
     * @return The total count of documents returned
     * @throws IOException If an I/O error occurs
     * @throws ProtocolException If a protocol error occurs
     */
    public int executeQueryAllPages(String query, int maxItemCount) throws IOException, ProtocolException {
        int totalDocumentCount = 0;
        String continuationToken = null;
        int pageCount = 0;
        
        // Generate a correlated activity ID that will be used across all requests
        String correlatedActivityId = UUID.randomUUID().toString();
        System.out.println("Generated correlated activity ID: " + correlatedActivityId);
        
        do {
            pageCount++;
            System.out.println("Fetching page " + pageCount + " of results");
            
            // Execute query with continuation token
            Page page = executeQueryWithContinuationAdhereToPageSize(query, maxItemCount, continuationToken, correlatedActivityId);
            continuationToken = page.getContinuationToken();
            
            // Count documents in this page
            if (page.getDocuments() != null) {
                int pageDocumentCount = page.getDocuments().size();
                totalDocumentCount += pageDocumentCount;
                System.out.println("Page " + pageCount + " contains " + pageDocumentCount + " documents. Total so far: " + totalDocumentCount);
            }
            
        } while (continuationToken != null);
        
        System.out.println("Query completed. Total pages: " + pageCount + ", Total documents: " + totalDocumentCount);
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
            System.out.println("Error generating authorization token: " + e.getMessage());
            throw new RuntimeException("Failed to generate authorization token", e);
        }
    }
    
    /**
     * Class to represent a page of query results.
     * Contains the raw response body, parsed documents, and continuation token.
     */
    public static class Page {
        private final List<Map<String, Object>> documents;
        private final String continuationToken;
        
        /**
         * Constructor for Page.
         *
         * @param documents The parsed documents from the response
         * @param continuationToken The continuation token for the next page
         */
        public Page(List<Map<String, Object>> documents, String continuationToken) {
            this.documents = documents;
            this.continuationToken = continuationToken;
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
            System.out.println("\n--- Query with where clause ---");
            String queryWithWhereClause = "SELECT * FROM c WHERE c.expectedProcessTime >= '2019-06-01T00:00' AND c.expectedProcessTime <= '2039-06-01T00:00'";

            int maxItemCount = 51;
            int totalItemCount = sample.executeQueryAllPages(queryWithWhereClause, maxItemCount);
            System.out.println("Query: " + queryWithWhereClause);
            System.out.println("Max Item Count: " + maxItemCount);
            System.out.println("Total documents: " + totalItemCount);
            
        } catch (IOException | ProtocolException e) {
            System.out.println("Error executing query: " + e.getMessage());
        }
    }

    /**
     * Creates an HTTP client with connection pooling.
     * 
     * @return A configured HTTP client
     */
    private static CloseableHttpClient createHttpClient() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(100);
        connectionManager.setDefaultMaxPerRoute(20);
        
        return HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();
    }
} 