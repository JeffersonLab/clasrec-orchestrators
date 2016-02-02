package org.jlab.clas.std.orchestrators;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.jlab.clas.std.orchestrators.errors.OrchestratorConfigError;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class ReconstructionConfigParserTest {

    private List<ServiceInfo> servicesList = new ArrayList<ServiceInfo>();
    private List<DpeInfo> recNodesList = new ArrayList<DpeInfo>();
    private List<DpeInfo> ioNodesList = new ArrayList<DpeInfo>();


    public ReconstructionConfigParserTest() {
        String defaultContainer = ReconstructionConfigParser.getDefaultContainer();
        servicesList.add(new ServiceInfo("org.jlab.clas12.ec.services.ECReconstruction",
                                         defaultContainer, "ECReconstruction"));
        servicesList.add(new ServiceInfo("org.clas12.services.tracking.SeedFinder",
                                         defaultContainer, "SeedFinder"));
        servicesList.add(new ServiceInfo("org.jlab.clas12.ftof.services.FTOFReconstruction",
                                         defaultContainer, "FTOFReconstruction"));

        String servicesDir = "/home/user/services";
        recNodesList.add(new DpeInfo("10.1.3.1_java", 12, servicesDir));
        recNodesList.add(new DpeInfo("10.1.3.2_java", 10, servicesDir));
        recNodesList.add(new DpeInfo("10.1.3.3_java", 12, servicesDir));

        ioNodesList.add(new DpeInfo("10.1.3.254_java", 0, "/home/user/clas12/services"));
    }


    @Rule
    public ExpectedException expectedEx = ExpectedException.none();


    @Test
    public void testGoodServicesFileYaml() {
        URL path = getClass().getResource("/services-ok.yaml");
        ReconstructionConfigParser parser = new ReconstructionConfigParser(path.getPath());
        List<ServiceInfo> services = parser.parseReconstructionChain();
        assertThat(services, is(servicesList));
    }


    @Test
    public void testBadServicesFileYaml() {
        expectedEx.expect(OrchestratorConfigError.class);
        expectedEx.expectMessage("missing name or class of service");

        URL path = getClass().getResource("/services-bad.yaml");
        ReconstructionConfigParser parser = new ReconstructionConfigParser(path.getPath());
        parser.parseReconstructionChain();
    }


    @Test
    public void testGoodNodesFileYaml() {
        URL path = getClass().getResource("/nodes-ok.yaml");
        ReconstructionConfigParser parser = new ReconstructionConfigParser(path.getPath());

        List<DpeInfo> recNodes = parser.parseReconstructionNodes();
        assertThat(recNodes, is(recNodesList));

        List<DpeInfo> ioNodes = parser.parseInputOutputNodes();
        assertThat(ioNodes, is(ioNodesList));
    }


    @Test
    public void testBadNodesFileYaml() {
        expectedEx.expect(OrchestratorConfigError.class);

        URL path = getClass().getResource("/nodes-bad.yaml");
        ReconstructionConfigParser parser = new ReconstructionConfigParser(path.getPath());

        expectedEx.expectMessage("missing name of reconstruction node");
        parser.parseReconstructionNodes();

        expectedEx.expectMessage("missing list of input-output nodes");
        parser.parseInputOutputNodes();
    }
}
