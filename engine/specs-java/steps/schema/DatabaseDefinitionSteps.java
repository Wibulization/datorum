package steps.schema;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;

public class DatabaseDefinitionSteps {
    @Given("^a Postgres database without schemas$")
    public void aSchemaWithATable() {
    }


    @And("an implementation of SchemaRepository")
    public void anImplementationOfSchemaRepository() {
    }

    @When("createBaseTables\\() is executed")
    public void createbasetablesIsExecuted() {
    }

    @Then("schema datorum_schema is created")
    public void schemaIsCreated() {
    }
}
