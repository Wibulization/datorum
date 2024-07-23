package io.beandev.datorum.migration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.lang.System.out;

@ExtendWith(MockitoExtension.class)
public class MigratorTest {
    @Mock
    private MigrationRepository repository;

    @Test
    void myTest() {
        out.println("hello");
        out.println("Testing " + repository.findAggregateSnapshot(null));
    }
}
