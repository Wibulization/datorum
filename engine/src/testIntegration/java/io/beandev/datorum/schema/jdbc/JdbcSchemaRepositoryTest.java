package io.beandev.datorum.schema.jdbc;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.beandev.datorum.data.AttributeRecord;
import io.beandev.datorum.data.BigId;
import io.beandev.datorum.data.EntityRecord;
import io.beandev.datorum.data.Event;
import io.beandev.datorum.migration.Difference;
import io.beandev.datorum.migration.Migration;
import io.beandev.datorum.migration.Migrator;
import io.beandev.datorum.migration.Scope;
import io.beandev.datorum.migration.jdbc.JdbcMigrationRepository;
import io.beandev.datorum.schema.Aggregate;
import io.beandev.datorum.schema.App;
import io.beandev.datorum.schema.Attribute;
import io.beandev.datorum.schema.Context;
import io.beandev.datorum.schema.Entity;

import static java.lang.System.out;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class JdbcSchemaRepositoryTest {
    private DataSource dataSource;
    private DSLContext create;

    @BeforeEach
    public void setup() throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/eventstore_db");
        hikariConfig.setUsername("postgres");
        hikariConfig.setPassword("password");

        HikariDataSource cp = new HikariDataSource(hikariConfig);
        cp.setMaximumPoolSize(12);
        cp.setMinimumIdle(2);

        this.dataSource = cp;
        try (Connection conn = cp.getConnection()) {
            this.create = DSL.using(conn, SQLDialect.POSTGRES);
        }
    }

    @AfterEach
    public void tearDown() {
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }

    private App createApp() {
        return new App(123, "Datorum");
    }

    private Context createContext(App app) {
        return new Context(456, "Datorum Context", app);
    }

    private Aggregate createAggregate(Context context) {
        return new Aggregate(789, "Datorum Aggregate", context);
    }

    private Entity createEntity(Aggregate aggregate) {
        return new Entity(0, "Datorum Entity", aggregate);
    }

    private Attribute createAttribute(Entity entity) {
        return new Attribute(
                1,
                "Datorum Attribute",
                new Attribute.DataType(Attribute.DataType.Type.STRING, 120),
                entity);
    }

    private AttributeRecord createAttributeRecord(Attribute attribute, EntityRecord entityRecord, String value) {
        return new AttributeRecord(new BigId(12), 1, attribute, entityRecord, value);
    }

    private AttributeRecord createAttributeIntRecord(Attribute attribute, EntityRecord entityRecord, Long value) {
        return new AttributeRecord(new BigId(12), 1, attribute, entityRecord, value);
    }

    @Test
    public void testAttributeCreation() {
        App app = createApp();
        Context context = createContext(app);
        Aggregate aggregate = createAggregate(context);
        Entity entity = createEntity(aggregate);
        Attribute attribute = createAttribute(entity);

        assertNotNull(attribute);
        assertEquals(1, attribute.id());
        assertEquals("Datorum Attribute", attribute.name());
        assertEquals(Attribute.DataType.Type.STRING, attribute.type().type());
        assertEquals(120, attribute.type().precisionOrLength());
    }

    @Test
    public void testAttributeRecordCreation() {
        App app = createApp();
        Context context = createContext(app);
        Aggregate aggregate = createAggregate(context);
        Entity entity = createEntity(aggregate);
        Attribute attribute = createAttribute(entity);

        AttributeRecord record = createAttributeRecord(attribute, null, "Test Value");

        assertNotNull(record);
        assertEquals(new BigId(12), record.id());
        assertEquals("Test Value", record.value().stringValue());
    }

    @Test
    public void testEventCreation() {
        App app = createApp();
        Context context = createContext(app);
        Aggregate aggregate = createAggregate(context);
        Entity entity = createEntity(aggregate);
        Attribute attribute = createAttribute(entity);

        AttributeRecord record = createAttributeRecord(attribute, null, "Datorum Value");

        Event event = new Event(new BigId(1),
                new Event.Operation[] { new Event.Operation(Event.Operator.CREATE, new Event.Operand(record)) });

        assertNotNull(event);
        assertEquals(new BigId(1), event.id());
        assertEquals(Event.Operator.CREATE, event.operations()[0].operator());

        ObjectMapper mapper = new ObjectMapper();

        try {
            String jsonString = mapper.writeValueAsString(record);
            assertNotNull(jsonString);
            assertTrue(jsonString.contains("Datorum Value"));
        } catch (Exception e) {
            fail("Serialization failed: " + e.getMessage());
        }
    }

    @Test
    public void testEventWithIntRecord() {
        App app = createApp();
        Context context = createContext(app);
        Aggregate aggregate = createAggregate(context);
        Entity entity = createEntity(aggregate);
        Attribute attribute = createAttribute(entity);

        EntityRecord entityValue = new EntityRecord(new BigId(1), entity, null, "Datorum Entity Value");

        AttributeRecord intRecord = createAttributeIntRecord(attribute, entityValue, 333L);

        Event event = new Event(new BigId(1),
                new Event.Operation[] { new Event.Operation(Event.Operator.CREATE, new Event.Operand(intRecord)) });

        assertNotNull(event);
        assertEquals(new BigId(1), event.id());
        assertEquals(Event.Operator.CREATE, event.operations()[0].operator());
        assertEquals(333L, intRecord.value().longValue());

        ObjectMapper mapper = new ObjectMapper();

        try {
            String jsonString = mapper.writeValueAsString(intRecord);
            assertNotNull(jsonString);
            assertTrue(jsonString.contains("Datorum Entity Value"));
        } catch (Exception e) {
            fail("Serialization failed: " + e.getMessage());
        }
    }

    @Test
    public void testDatabaseConnection() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            assertNotNull(conn);
            assertTrue(conn.isValid(2));
        }
    }

    // @Test
    // public void testMigrationCreationAndApplication() throws SQLException {
    // App app = createApp();
    // Context context = createContext(app);
    // Aggregate aggregate = createAggregate(context);

    // Migration migration = new Migration(
    // 2,
    // 2024071533478L,
    // new Difference[] {
    // new Difference(
    // 1,
    // "name2",
    // Scope.AGGREGATE)
    // });

    // try (Connection conn = dataSource.getConnection()) {
    // JdbcMigrationRepository migrationRepository = new
    // JdbcMigrationRepository(dataSource);
    // migrationRepository.createBaseTables();

    // Migrator migrator = new Migrator(migrationRepository);
    // migrator.apply(migration);

    // // var appliedMigrations = migrationRepository;
    // // assertFalse(appliedMigrations.isEmpty());
    // // assertEquals(1, appliedMigrations.size());
    // // assertEquals(migration.id(), appliedMigrations.get(0).id());
    // }
    // }

}
