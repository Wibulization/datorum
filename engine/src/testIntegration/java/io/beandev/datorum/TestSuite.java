package io.beandev.datorum;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

import io.beandev.datorum.migration.jdbc.JdbcMigrationRepositoryTest;
import io.beandev.datorum.schema.jdbc.JdbcSchemaRepositoryTest;

@TestInstance(Lifecycle.PER_CLASS)
@Suite
public class TestSuite {

    @BeforeAll
    public static void createDB() throws Exception {
        System.out.println("Setting up the database...");
        CreateDatabase.getInstance();
    }

    @AfterAll
    public static void deleteDB() throws Exception {
        System.out.println("Dropping down the database...");
        CreateDatabase.removeDatabase();
    }
}
