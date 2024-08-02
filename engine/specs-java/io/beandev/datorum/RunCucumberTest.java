package io.beandev.datorum;

import io.cucumber.java.AfterAll;
import io.cucumber.java.Before;
import io.cucumber.java.BeforeAll;
import io.cucumber.java.Scenario;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;
import static java.lang.System.out;

@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource(".")
@ConfigurationParameter(key = PLUGIN_PROPERTY_NAME, value = "pretty")
@ConfigurationParameter(key = "cucumber.filter.tags", value = "not @e2e")
public class RunCucumberTest {
    @BeforeAll
    public static void beforeAll() {
        out.print("Only once beforeAll");
    }

    @Before
    public void before(Scenario scenario) {
        out.println("before each Integration Test Scenario for scenario " + scenario.getName());
        out.println("Mr Bean is " + System.getProperty("bean"));
        System.setProperty("TEST", "INTEGRATION");
    }

    @AfterAll
    public static void afterAll() {
        out.print("Only once afterAll");
    }
}
