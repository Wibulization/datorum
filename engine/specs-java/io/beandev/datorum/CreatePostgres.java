package io.beandev.datorum;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1HostPathVolumeSource;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1PersistentVolumeSpec;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeResourceRequirements;
import io.kubernetes.client.util.Config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class CreatePostgres {

    private static CreatePostgres instance;
    private final CoreV1Api api;

    public CreatePostgres() throws Exception {
        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);
        this.api = new CoreV1Api();
    }

    public static CreatePostgres getInstance() throws Exception {
        if (instance == null) {
            instance = new CreatePostgres();
        }
        return instance;
    }

    public void initDatabase() throws Exception {

        ensurePostgresServiceExists(api);

        waitForPostgresServiceReady(api, "default", "postgres-service");

        ensurePostgresPVExists(api, "postgres-pv");

        ensurePostgresPVCExists(api, "default", "postgres-persistent-volume-claim");

        ensurePostgresPodExists(api, "default", "postgres");

        waitForPostgresPodReady(api, "default", "postgres");

    }

    private static V1Pod createPostgresPodDefinition() {
        return new V1Pod()
                .apiVersion("v1")
                .kind("Pod")
                .metadata(new V1ObjectMeta().name("postgres").labels(Map.of("app", "postgres")))
                .spec(new V1PodSpec()
                        .overhead(null)
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
                                                .value("password")))
                                .volumeMounts(Collections.singletonList(new V1VolumeMount()
                                        .name("postgresdb")
                                        .mountPath("/var/lib/postgresql/data")))))
                        .volumes(Collections.singletonList(new V1Volume()
                                .name("postgresdb")
                                .persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
                                        .claimName("postgres-persistent-volume-claim")))));
    }

    private static V1Service createPostgresServiceDefinition() {
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
                                        .nodePort(30000)
                                        .protocol("TCP"))));
    }

    private static V1PersistentVolume createPostgresPVDefinition() {
        return new V1PersistentVolume()
                .apiVersion("v1")
                .kind("PersistentVolume")
                .metadata(new V1ObjectMeta().name("postgres-pv").labels(Map.of("type", "local")))
                .spec(new V1PersistentVolumeSpec()
                        .capacity(Map.of("storage", new Quantity("1Gi")))
                        .accessModes(Collections.singletonList("ReadWriteOnce"))
                        .persistentVolumeReclaimPolicy("Retain")
                        .storageClassName("manual")
                        .hostPath(new V1HostPathVolumeSource()
                                .path("/Users/Khoi-Kun/Downloads/Homework/HowToUnderstand/vol"))); // Replace this with
                                                                                                   // an actual path on
                                                                                                   // your node
    }

    private static V1PersistentVolumeClaim createPostgresPVCDefinition() {
        return new V1PersistentVolumeClaim()
                .apiVersion("v1")
                .kind("PersistentVolumeClaim")
                .metadata(new V1ObjectMeta().name("postgres-persistent-volume-claim"))
                .spec(new V1PersistentVolumeClaimSpec()
                        .accessModes(Collections.singletonList("ReadWriteOnce"))
                        .resources(new V1VolumeResourceRequirements()
                                .requests(Map.of("storage", Quantity.fromString("1Gi"))))
                        .storageClassName("manual"));
    }

    private static void waitForPostgresServiceReady(CoreV1Api api, String namespace, String serviceName)
            throws Exception {
        long serviceTimeoutSeconds = 30;
        long startTime = System.currentTimeMillis();
        while (true) {
            V1Service service = api.readNamespacedService(serviceName, namespace).execute();
            if (service.getStatus() != null && service.getStatus().getLoadBalancer() != null) {
                System.out.println("PostgreSQL Service is ready.");
                break;
            } else {
                long currentTime = System.currentTimeMillis();
                long elapsedSeconds = (currentTime - startTime) / 1000;
                if (elapsedSeconds > serviceTimeoutSeconds) {
                    throw new Exception("Timeout : PostgreSQL Service is not ready in 30 seconds");
                }
                System.out.println("PostgreSQL Service is not ready, waiting...");
                TimeUnit.SECONDS.sleep(5);
            }
        }
    }

    private static void waitForPostgresPodReady(CoreV1Api api, String namespace, String podName) throws Exception {
        long podTimeoutSeconds = 60;
        long startTime = System.currentTimeMillis();
        while (true) {
            V1Pod pod = api.readNamespacedPod(podName, namespace).execute();
            if (pod.getStatus() != null && pod.getStatus().getPhase().equals("Running")) {
                System.out.println("PostgreSQL Pod is ready.");
                break;
            } else {
                long currentTime = System.currentTimeMillis();
                long elapsedSeconds = (currentTime - startTime) / 1000;
                if (elapsedSeconds > podTimeoutSeconds) {
                    throw new Exception("Timeout : PostgreSQL Pod is not ready in 1 minute");
                }
                System.out.println("PostgreSQL Pod is not ready, waiting...");
                TimeUnit.SECONDS.sleep(30);
            }
        }
    }

    private static void ensurePostgresPodExists(CoreV1Api api, String namespace, String podName) throws Exception {
        try {
            V1Pod existingPod = api.readNamespacedPod(podName, namespace).execute();
            System.out.println("PostgreSQL Pod already exists: " + existingPod.getMetadata().getName());
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                V1Pod postgresPodDef = createPostgresPodDefinition();
                V1Pod createdPod = api.createNamespacedPod(namespace, postgresPodDef).execute();
                System.out.println("PostgreSQL Pod created: " + createdPod.getMetadata().getName());
            } else {
                throw e;
            }
        }
    }

    private static void ensurePostgresPVExists(CoreV1Api api, String persistentVolumeName) throws Exception {
        try {
            V1PersistentVolume existingPersistentVolume = api.readPersistentVolume(persistentVolumeName).execute();
            System.out.println(
                    "PostgreSQL PersistentVolume already exists: " + existingPersistentVolume.getMetadata().getName());
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                V1PersistentVolume postgresPersistentVolumeDef = createPostgresPVDefinition();
                V1PersistentVolume createdPersistentVolume = api.createPersistentVolume(postgresPersistentVolumeDef)
                        .execute();
                System.out.println("PostgreSQL Pod created: " + createdPersistentVolume.getMetadata().getName());
            } else {
                throw e;
            }
        }
    }

    private static void ensurePostgresPVCExists(CoreV1Api api, String namespace, String persistentVolumeClaimName)
            throws Exception {
        try {
            V1PersistentVolumeClaim existingPersistentVolumeClaim = api
                    .readNamespacedPersistentVolumeClaim(persistentVolumeClaimName, namespace).execute();
            System.out.println(
                    "PostgreSQL PersistentVolumeClaim already exists: "
                            + existingPersistentVolumeClaim.getMetadata().getName());
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                V1PersistentVolumeClaim postgresPersistentVolumeClaimDef = createPostgresPVCDefinition();
                V1PersistentVolumeClaim createdPersistentVolumeClaim = api
                        .createNamespacedPersistentVolumeClaim(namespace, postgresPersistentVolumeClaimDef).execute();
                System.out.println("PostgreSQL Pod created: " + createdPersistentVolumeClaim.getMetadata().getName());
            } else {
                throw e;
            }
        }
    }

    private static void ensurePostgresServiceExists(CoreV1Api api)
            throws Exception {
        try {
            V1Service existingService = api.readNamespacedService("postgres-service", "default").execute();
            System.out.println("PostgreSQL Service already exists: "
                    + Objects.requireNonNull(existingService.getMetadata()).getName());
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                V1Service postgresServiceDef = createPostgresServiceDefinition();
                V1Service createdService = api.createNamespacedService("default", postgresServiceDef).execute();
                System.out.println("PostgreSQL Service created: "
                        + Objects.requireNonNull(createdService.getMetadata()).getName());
            } else {
                throw e;
            }
        }
    }

}
