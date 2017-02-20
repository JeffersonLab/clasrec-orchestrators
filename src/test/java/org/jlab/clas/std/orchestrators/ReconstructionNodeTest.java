package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.ServiceName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ReconstructionNodeTest {

    @Mock
    private ReconstructionOrchestrator orchestrator;

    @Captor
    ArgumentCaptor<DeployInfo> deployCaptor;

    @Captor
    private ArgumentCaptor<DpeName> dpeCaptor;

    @Captor
    private ArgumentCaptor<Set<ServiceName>> namesCaptor;

    private ReconstructionNode node;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        doReturn(true).when(orchestrator).findServices(any(), any());
    }


    @Test
    public void deployServicesSendsAllRequests() throws Exception {
        node = new ReconstructionNode(orchestrator, SingleLangData.application());

        node.deployServices();

        verify(orchestrator, times(7)).deployService(deployCaptor.capture());

        assertThat(deployCaptor.getAllValues(),
                   containsInAnyOrder(SingleLangData.expectedDeploys()));
    }


    @Test
    public void deployServicesChecksSingleDpe() throws Exception {
        node = new ReconstructionNode(orchestrator, SingleLangData.application());

        node.deployServices();

        verify(orchestrator, times(1)).checkServices(dpeCaptor.capture(), namesCaptor.capture());

        assertThat(dpeCaptor.getValue(), is(SingleLangData.expectedDpe()));
    }


    @Test
    public void deployServicesChecksAllServices() throws Exception {
        node = new ReconstructionNode(orchestrator, SingleLangData.application());

        node.deployServices();

        verify(orchestrator, times(1)).checkServices(dpeCaptor.capture(), namesCaptor.capture());

        assertThat(namesCaptor.getValue(),
                   containsInAnyOrder(SingleLangData.expectedServices()));
    }


    @Test
    public void checkServicesQueriesSingleDpe() throws Exception {
        node = new ReconstructionNode(orchestrator, SingleLangData.application());

        node.checkServices();

        verify(orchestrator, times(1)).findServices(dpeCaptor.capture(), namesCaptor.capture());

        assertThat(dpeCaptor.getValue(), is(SingleLangData.expectedDpe()));
    }


    @Test
    public void checkServicesQueriesAllServices() throws Exception {
        node = new ReconstructionNode(orchestrator, SingleLangData.application());

        node.checkServices();

        verify(orchestrator, times(1)).findServices(dpeCaptor.capture(), namesCaptor.capture());

        assertThat(namesCaptor.getValue(),
                   containsInAnyOrder(SingleLangData.expectedServices()));
    }


    private static class SingleLangData {

        static ReconstructionApplication application() {
            return AppData.builder()
                          .withServices(AppData.J1, AppData.J2, AppData.K1, AppData.K2)
                          .build();
        }

        static DeployInfo[] expectedDeploys() {
            return new DeployInfo[] {
                deploy("10.1.1.10_java:master:S1", "org.test.S1", 1),
                deploy("10.1.1.10_java:master:R1", "org.test.R1", 1),
                deploy("10.1.1.10_java:master:W1", "org.test.W1", 1),
                deploy("10.1.1.10_java:master:J1", "org.test.J1", AppData.CORES),
                deploy("10.1.1.10_java:master:J2", "org.test.J2", AppData.CORES),
                deploy("10.1.1.10_java:slave:K1", "org.test.K1", AppData.CORES),
                deploy("10.1.1.10_java:slave:K2", "org.test.K2", AppData.CORES),
            };
        }

        static DpeName expectedDpe() {
            return new DpeName("10.1.1.10_java");
        }

        static ServiceName[] expectedServices() {
            return new ServiceName[] {
                new ServiceName("10.1.1.10_java:master:S1"),
                new ServiceName("10.1.1.10_java:master:R1"),
                new ServiceName("10.1.1.10_java:master:W1"),
                new ServiceName("10.1.1.10_java:master:J1"),
                new ServiceName("10.1.1.10_java:master:J2"),
                new ServiceName("10.1.1.10_java:slave:K1"),
                new ServiceName("10.1.1.10_java:slave:K2"),
            };
        }
    }


    private static DeployInfo deploy(String name, String classPath, int poolSize) {
        return new DeployInfo(new ServiceName(name), classPath, poolSize);
    }
}
