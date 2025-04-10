# CosmosDB REST API Samples

This project contains sample code demonstrating how to interact with Azure Cosmos DB using REST APIs in Java.

## Project Structure

```
.
├── pom.xml                 # Maven project configuration
└── src
    └── main
        └── java
            └── com
                └── example
                    └── cosmosdb
                        └── Application.java
```

## Requirements

- Java 17 or higher
- Maven 3.6 or higher

## Building the Project

To build the project, run:

```bash
mvn clean install
```

## Running the Application

To run the application, use:

```bash
mvn exec:java -Dexec.mainClass="com.example.cosmosdb.Application"
```