package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.ServiceName;
import org.junit.Test;

import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ReconstructionApplicationTest {

    @Test
    public void getStageService() throws Exception {
        ReconstructionApplication app = AppData.builder().build();

        assertThat(app.stageService().canonicalName(), is("10.1.1.10_java:master:S1"));
    }


    @Test
    public void getReaderService() throws Exception {
        ReconstructionApplication app = AppData.builder().build();

        assertThat(app.readerService().canonicalName(), is("10.1.1.10_java:master:R1"));
    }


    @Test
    public void getWriterService() throws Exception {
        ReconstructionApplication app = AppData.builder().build();

        assertThat(app.writerService().canonicalName(), is("10.1.1.10_java:master:W1"));
    }


    @Test
    public void getReconstructionServices() throws Exception {
        ReconstructionApplication app = AppData.builder().build();

        ServiceName[] expected = toServices("10.1.1.10_java:master:J1",
                                            "10.1.1.10_java:master:J2",
                                            "10.1.1.10_java:master:J3");

        assertThat(app.recServices(), containsInAnyOrder(expected));
    }


    @Test
    public void getUniqueContainer() throws Exception {
        ReconstructionApplication app = AppData.builder().build();

        ContainerName[] expected = toContainers("10.1.1.10_java:master");

        assertThat(app.allContainers(), containsInAnyOrder(expected));
    }


    @Test
    public void getAllContainers() throws Exception {
        ReconstructionApplication app = AppData.builder()
                .withServices(AppData.J1, AppData.J2, AppData.K1)
                .build();

        ContainerName[] expected = toContainers("10.1.1.10_java:master", "10.1.1.10_java:slave");

        assertThat(app.allContainers(), containsInAnyOrder(expected));
    }


    private static ServiceName[] toServices(String... elem) {
        return Stream.of(elem)
                     .map(ServiceName::new)
                     .toArray(ServiceName[]::new);
    }


    private static ContainerName[] toContainers(String... elem) {
        return Stream.of(elem)
                     .map(ContainerName::new)
                     .toArray(ContainerName[]::new);
    }
}
