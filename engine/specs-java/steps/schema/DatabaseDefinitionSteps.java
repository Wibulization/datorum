package steps.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.beandev.datorum.CreatePostgres;
import io.beandev.datorum.schema.jdbc.JdbcSchemaRepository;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;

public class DatabaseDefinitionSteps {
    private JdbcSchemaRepository schema;
    private HikariDataSource dataSource;

    @Given("^a Postgres database without schemas$")
    public void aSchemaWithATable() throws Exception {
        // Create Database
        CreatePostgres.getInstance();
    }

    @And("an implementation of SchemaRepository")
    public void anImplementationOfSchemaRepository() {
        schema = implementationOfSchema();
        if (schema == null)
            System.out.println("Fail to connect PostgresDB");
    }

    @When("createBaseTables\\() is executed")
    public void createbasetablesIsExecuted() {
        schema.createBaseTables();
    }

    @Then("schema datorum_schema is created")
    public void schemaIsCreated() {
        checkSchema();
    }

    private JdbcSchemaRepository implementationOfSchema() {
        dataSource = dataSource();

        JdbcSchemaRepository schemaRepository = new JdbcSchemaRepository(dataSource);

        return schemaRepository;
    }

    private HikariDataSource dataSource() {
        String userName = "postgres";
        String password = "password";
        String url = "jdbc:postgresql://127.0.0.1:32543/eventstore_db";

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(userName);
        hikariConfig.setPassword(password);

        HikariDataSource cp = new HikariDataSource(hikariConfig);
        cp.setMaximumPoolSize(12);
        cp.setMinimumIdle(2);

        return cp;
    }

    private void checkSchema() {
        // Verify schema 'datorum_schema'
        String query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'datorum_schema'";

        try (Connection con = dataSource.getConnection();
                PreparedStatement pst = con.prepareStatement(query);
                ResultSet rs = pst.executeQuery()) {

            if (rs.next()) {
                System.out.println("Schema 'datorum_schema' exists.");
            } else {
                System.out.println("Schema 'datorum_schema' does not exist.");
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }
}
