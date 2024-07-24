package io.beandev.datorum;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.util.Config;

public class CreateDatabase {
    public CreateDatabase() {
        try {
            initDatabase();
        } catch (Exception e) {
            System.out.println("Exception : " + e);
        }
    }

    public static void initDatabase() throws Exception {
        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);
        System.out.println("Kubernetes client has been configured");

        // Tạo CoreV1Api để thực hiện các thao tác với Core API (Pods, Services, v.v.)
        CoreV1Api api = new CoreV1Api();

        try {
            V1Service existingService = api.readNamespacedService("postgres-service", "default", null);
            System.out.println("Service already exists: " + existingService.getMetadata().getName());
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                // Service does not exist, create it
                V1Service service = createService();
                V1Service createService = api.createNamespacedService("default", service, null, null, null, null);
                System.out.println("NodePort Service created: " + createService.getMetadata().getName());
            } else {
                throw e; // Rethrow other API exceptions
            }
        }

        // Wait for the Service to be ready
        waitForServiceReady(api, "default", "postgres-service");

        // Check if Pod already exists
        try {
            V1Pod existingPod = api.readNamespacedPod("postgres", "default", null);
            System.out.println("Pod already exists: " + existingPod.getMetadata().getName());
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                // Pod does not exist, create it
                V1Pod pod = createPostgresPod();
                V1Pod createdPod = api.createNamespacedPod("default", pod, null, null, null, null);
                System.out.println("PostgreSQL Pod created: " + createdPod.getMetadata().getName());
            } else {
                throw e; // Rethrow other API exceptions
            }
        }

        waitForPodReady(api, "default", "postgres");

        String dbUrl = "jdbc:postgresql://localhost:32543/"; // Connect to default database
        String username = "postgres";
        String password = "password";

        try (Connection connection = DriverManager.getConnection(dbUrl, username, password)) {
            // Create a new database `db_test`
            createDatabase(connection, "eventstore_db");
        } catch (SQLException e) {
            System.err.println("Database connection error to db_test: " + e.getMessage());
        }

    }

    // Method to create PostgreSQL Pod
    private static V1Pod createPostgresPod() {
        return new V1Pod()
                .apiVersion("v1")
                .kind("Pod")
                .metadata(new V1ObjectMeta().name("postgres").labels(Map.of("app", "postgres")))
                .spec(new V1PodSpec()
                        .containers(Collections.singletonList(new V1Container()
                                .name("postgres")
                                .image("postgres:latest")
                                .ports(Collections.singletonList(new V1ContainerPort()
                                        .containerPort(5432)))
                                .env(Arrays.asList(
                                        new V1EnvVar().name("POSTGRES_DB")
                                                .value("postgres"),
                                        new V1EnvVar().name("POSTGRES_USER")
                                                .value("postgres"),
                                        new V1EnvVar().name("POSTGRES_PASSWORD")
                                                .value("password"))))));
    }

    // Method to create a Kubernetes Service
    private static V1Service createService() {
        return new V1Service()
                .apiVersion("v1")
                .kind("Service")
                .metadata(new V1ObjectMeta().name("postgres-service"))
                .spec(new V1ServiceSpec()
                        .type("NodePort")
                        .selector(Map.of("app", "postgres"))
                        .ports(List.of(
                                new V1ServicePort()
                                        .port(5432)
                                        .targetPort(new IntOrString(5432))
                                        .nodePort(32543)
                                        .protocol("TCP"))));
    }

    // Method to check if the Service is ready
    private static void waitForServiceReady(CoreV1Api api, String namespace, String serviceName) throws Exception {
        while (true) {
            V1Service service = api.readNamespacedService(serviceName, namespace, null);
            if (service.getStatus() != null && service.getStatus().getLoadBalancer() != null) {
                System.out.println("Service is ready.");
                break;
            } else {
                System.out.println("Service is not ready, waiting...");
                TimeUnit.SECONDS.sleep(5);
            }
        }
    }

    // Method to check if the Pod is ready
    private static void waitForPodReady(CoreV1Api api, String namespace, String podName) throws Exception {
        while (true) {
            V1Pod pod = api.readNamespacedPod(podName, namespace, null);
            if (pod.getStatus() != null && pod.getStatus().getPhase().equals("Running")) {
                System.out.println("Pod is ready.");
                break;
            } else {
                System.out.println("Pod is not ready, waiting...");
                TimeUnit.SECONDS.sleep(30);
            }
        }
    }

    // Method to create a new database
    private static void createDatabase(Connection connection, String dbName) {
        try (Statement stmt = connection.createStatement()) {
            // Check if the database already exists
            ResultSet rs = stmt.executeQuery("SELECT 1 FROM pg_database WHERE datname = '" + dbName + "'");

            if (rs.next()) {
                System.out.println("Database " + dbName + " already exists.");
            } else {
                // Create the database if it does not exist
                String createDatabase = "CREATE DATABASE " + dbName;
                stmt.executeUpdate(createDatabase);
                System.out.println("Database " + dbName + " created successfully.");
            }
        } catch (SQLException e) {
            System.err.println("Error executing SQL to create database: " + e.getMessage());
        }
    }

}
