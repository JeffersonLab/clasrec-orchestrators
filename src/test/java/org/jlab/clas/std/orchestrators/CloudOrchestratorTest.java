package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.DpeName;
import org.jlab.clas.std.orchestrators.AbstractOrchestrator.ReconstructionOptions;
import org.jlab.clas.std.orchestrators.CloudOrchestrator.DpeReportCB;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CloudOrchestratorTest {

    private final DpeInfo frontEnd = AppData.dpe("10.1.1.254_java");
    private final ExecutorService dpeExecutor = Executors.newFixedThreadPool(3);

    private ReconstructionOrchestrator orchestrator;


    @Before
    public void setup() {
        orchestrator = mock(ReconstructionOrchestrator.class);
        when(orchestrator.getFrontEnd()).thenReturn(frontEnd.name);
    }


    @Test
    public void useSingleNode() throws Exception {
        NodeData data = buildNodeData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.1_java");

        assertThat(cb.nodes(), contains(data.nodes("10.1.1.1")));
    }


    @Test
    public void useMultipleNodes() throws Exception {
        NodeData data = buildNodeData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.2_java");

        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.1_java");

        ReconstructionNode[] expected = data.nodes("10.1.1.1", "10.1.1.2", "10.1.1.3");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void singleNodeUsingFrontEnd() throws Exception {
        NodeData data = buildNodeData();
        DpeReportCBTest cb = data.callback(true, 10);

        cb.callback("10.1.1.254_java");

        assertThat(cb.nodes(), contains(data.nodes("10.1.1.254")));
    }


    @Test
    public void multipleNodesUsingFrontEnd() throws Exception {
        NodeData data = buildNodeData();
        DpeReportCBTest cb = data.callback(true, 10);

        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.2_java");

        ReconstructionNode[] expected = data.nodes("10.1.1.254", "10.1.1.1", "10.1.1.2");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void singleNodeIgnoringFrontEnd() throws Exception {
        NodeData data = buildNodeData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.254_java");

        assertThat(cb.nodes(), is(empty()));
    }


    @Test
    public void multipleNodesIgnoringFrontEnd() throws Exception {
        NodeData data = buildNodeData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.254_java");

        ReconstructionNode[] expected = data.nodes("10.1.1.1", "10.1.1.2");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void limitNodesUsingFrontEnd() throws Exception {
        NodeData data = buildNodeData();
        DpeReportCBTest cb = data.callback(true, 3);

        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.3_java");

        cb.waitCallbacks();

        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.1_java");

        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.4_java");

        Set<ReconstructionNode> actual = new HashSet<>(cb.nodes());

        assertThat(actual, iterableWithSize(3));
        assertThat(actual, hasItem(data.node("10.1.1.254")));
    }


    @Test
    public void limitNodesIgnoringFrontEnd() throws Exception {
        NodeData data = buildNodeData();
        DpeReportCBTest cb = data.callback(false, 3);

        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.3_java");

        cb.callback("10.1.1.5_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.254_java");

        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.4_java");

        Set<ReconstructionNode> actual = new HashSet<>(cb.nodes());

        assertThat(actual, iterableWithSize(3));
        assertThat(actual, not(hasItem(data.node("10.1.1.254"))));
    }


    private NodeData buildNodeData() {
        return new NodeData();
    }


    private class NodeData {

        DpeReportCBTest callback(boolean useFrontEnd, int maxNodes) {
            return new DpeReportCBTest(useFrontEnd, maxNodes);
        }

        ReconstructionNode node(String host) {
            return new ReconstructionNode(orchestrator, dpe(host));
        }

        ReconstructionNode[] nodes(String... hosts) {
            return Stream.of(hosts).map(this::node).toArray(ReconstructionNode[]::new);
        }

        private DpeInfo dpe(String host) {
            DpeName name = new DpeName(host, ClaraLang.JAVA);
            return AppData.dpe(name.canonicalName());
        }
    }


    private class DpeReportCBTest {

        private final List<Callable<Object>> tasks;
        private final List<ReconstructionNode> nodes;
        private final Consumer<ReconstructionNode> nodeConsumer;
        private final DpeReportCB callback;

        DpeReportCBTest(boolean useFrontEnd, int maxNodes) {
            tasks = Collections.synchronizedList(new ArrayList<>());
            nodes = Collections.synchronizedList(new ArrayList<>());
            nodeConsumer = node -> nodes.add(node);
            callback = new DpeReportCB(orchestrator, options(useFrontEnd, maxNodes), nodeConsumer);
        }

        public void callback(String dpeName) {
            tasks.add(Executors.callable(() -> callback.callback(AppData.dpe(dpeName))));
        }

        public void waitCallbacks() throws Exception {
            dpeExecutor.invokeAll(tasks);
            tasks.clear();
        }

        public List<ReconstructionNode> nodes() throws Exception {
            waitCallbacks();
            return nodes;
        }
    }


    private static ReconstructionOptions options(boolean useFrontEnd, int maxNodes) {
        return new ReconstructionOptions(useFrontEnd, false, false, 1, maxNodes, 1);
    }
}
