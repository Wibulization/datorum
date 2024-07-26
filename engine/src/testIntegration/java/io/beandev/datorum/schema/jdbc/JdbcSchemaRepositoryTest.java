package io.beandev.datorum.schema.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.beandev.datorum.CreateDatabase;

import static java.lang.System.out;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcSchemaRepositoryTest {
    private DataSource dataSource;
    private JdbcSchemaRepository jdbcSchemaRepository;

    @BeforeAll
    public static void createDB() throws Exception {
        new CreateDatabase();
    }

    @BeforeEach
    public void setup() throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:postgresql://127.0.0.1:32543/eventstore_db");
        hikariConfig.setUsername("postgres");
        hikariConfig.setPassword("password");

        HikariDataSource cp = new HikariDataSource(hikariConfig);
        cp.setMaximumPoolSize(12);
        cp.setMinimumIdle(2);
        this.dataSource = cp;

        // Thử mở kết nối để kiểm tra cấu hình
        try (Connection testConnection = dataSource.getConnection()) {
            if (testConnection != null) {
                System.out.println("HikariCP configuration is valid and connection is successful.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail("HikariCP configuration is invalid or connection failed: " + e.getMessage());
        }

        this.jdbcSchemaRepository = new JdbcSchemaRepository(dataSource);

        // Cleanup the database before running tests
        try (Connection conn = dataSource.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP SCHEMA IF EXISTS datorum_schema CASCADE");
            }
        }
    }

    @AfterEach
    public void tearDown() {
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }

    @Test
    public void testJdbcSchemaRepositoryCreation() {
        try {
            assertNotNull(jdbcSchemaRepository, "JdbcSchemaRepository should be successfully instantiated");
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception occurred while testing JdbcSchemaRepository creation: " + e.getMessage());
        }
    }

    @Test
    public void testSchemaCreation() {
        try {
            // Create schema and tables
            jdbcSchemaRepository.createBaseTables();

            // Verify schema creation
            try (Connection conn = dataSource.getConnection()) {
                DatabaseMetaData metaData = conn.getMetaData();
                try (ResultSet rs = metaData.getSchemas(null, "datorum_schema")) {
                    assertTrue(rs.next(), "Schema 'datorum_schema' should exist after creation");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Exception occurred while testing schema creation: " + e.getMessage());
        }
    }

    @Test
    public void testSchemaCreationFailure() {
        try {
            // Xác minh rằng schema không được tạo ra (dự kiến lỗi sẽ xảy ra)
            try (Connection conn = dataSource.getConnection()) {
                DatabaseMetaData metaData = conn.getMetaData();
                try (ResultSet rs = metaData.getSchemas(null, "datorum_schema")) {
                    // Đây là một lỗi dự kiến, vì chúng ta mong muốn schema không tồn tại
                    assertFalse(rs.next(), "Schema 'datorum_schema' should not exist after failed creation");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Exception occurred while testing schema creation failure: " + e.getMessage());
        }
    }

    @Test
    public void testCheckIntoSystemInfo() {

        // Tạo các bảng cơ sở
        jdbcSchemaRepository.createBaseTables();

        try (Connection conn = dataSource.getConnection()) {
            // Verify data insertion
            String selectSql = "SELECT property_name, property_value FROM datorum_schema.system_info";
            try (PreparedStatement pstmt = conn.prepareStatement(selectSql)) {
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next(), "Row should exist");
                    // Kiểm tra giá trị của property_value
                    String propertyName = rs.getString("property_name");
                    String propertyValue = rs.getString("property_value");

                    Assertions.assertEquals("schema.version", propertyName,
                            "The property_name should be 'schema.version'");
                    Assertions.assertEquals("v1.0.0", propertyValue, "The property_value should be 'v1.0.0'");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assertions.fail("Exception occurred while testing data insertion: " + e.getMessage());
        }
    }

    @Test
    public void testCheckFailType() {

        // Tạo các bảng cơ sở
        jdbcSchemaRepository.createBaseTables();

        try (Connection conn = dataSource.getConnection()) {
            // Verify type creation
            DatabaseMetaData metaData = conn.getMetaData();

            // Replace 'your_type_name' with the actual type name you expect
            String[] typeNames = { "your_type_name1", "your_type_name2" };

            for (String typeName : typeNames) {
                try (ResultSet rs = metaData.getUDTs(null, "datorum_schema", typeName, null)) {
                    assertFalse(rs.next(), "Type '" + typeName + "' should not exist after creation");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assertions.fail("Exception occurred while testing : " + e.getMessage());
        }
    }

    @Test
    public void testCheckFailTable() {

        // Tạo các bảng cơ sở
        jdbcSchemaRepository.createBaseTables();

        try (Connection conn = dataSource.getConnection()) {
            // Verify type creation
            DatabaseMetaData metaData = conn.getMetaData();

            String[] tableNames = { "table1", "table2", "test3" };

            for (String table : tableNames) {
                try (ResultSet rs = metaData.getTables(null, "datorum_schema", table, new String[] { "TABLE" })) {
                    assertFalse(rs.next(), "Table '" + table + "' should not exist after creation");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assertions.fail("Exception occurred while testing : " + e.getMessage());
        }
    }

}