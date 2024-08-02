Feature: Schema DB
  Rule: WHILE base tables are missing, THEN create them
    The schema is not empty
    Scenario: Empty database
        Given a Postgres database without schemas
        And an implementation of SchemaRepository
        When createBaseTables() is executed
        Then schema datorum_schema is created