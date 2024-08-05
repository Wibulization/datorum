package io.beandev.datorum;

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

public class CreatePostgres {

    private static CreatePostgres instance;
    private CoreV1Api api;

    public CreatePostgres() throws Exception {
        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);
        // Create CoreV1Api to perform operations with Core API
        this.api = new CoreV1Api();
        initDatabase();
    }

    public static CreatePostgres getInstance() throws Exception {
        if (instance == null) {
            instance = new CreatePostgres();
        }
        return instance;
    }

    private void initDatabase() throws Exception {

        // Check if Service already exists
        checkServiceExistsAndCreateService(api, "default", "postgres-service");

        // Wait for the Service to be ready
        waitForServiceReady(api, "default", "postgres-service");

        // Check if Pod already exists
        checkPodExistsAndCreatePod(api, "default", "postgres");

        // Wait for the Pod to be ready
        waitForPodReady(api, "default", "postgres");

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
                                                .value("eventstore_db"),
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

    // Method to check and create pod
    private static void checkPodExistsAndCreatePod(CoreV1Api api, String namespace, String podName) throws Exception {
        try {
            V1Pod existingPod = api.readNamespacedPod(podName, namespace, null);
            System.out.println("Pod already exists: " + existingPod.getMetadata().getName());
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                // Pod does not exist, create it
                V1Pod pod = createPostgresPod();
                V1Pod createdPod = api.createNamespacedPod(namespace, pod, null, null, null, null);
                System.out.println("PostgreSQL Pod created: " + createdPod.getMetadata().getName());
            } else {
                throw e; // Rethrow other API exceptions
            }
        }
    }

    // Method to check and create service
    private static void checkServiceExistsAndCreateService(CoreV1Api api, String namespace, String serviceName)
            throws Exception {
        try {
            V1Service existingService = api.readNamespacedService(serviceName, namespace, null);
            System.out.println("Service already exists: " + existingService.getMetadata().getName());
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                // Service does not exist, create it
                V1Service service = createService();
                V1Service createdService = api.createNamespacedService(namespace, service, null, null, null, null);
                System.out.println("Service created: " + createdService.getMetadata().getName());
            } else {
                throw e; // Rethrow other API exceptions
            }
        }
    }

}
