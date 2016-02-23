package org.jlab.clas.std.orchestrators;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas.std.orchestrators.ReconstructionConfigParser.ConfigFileChecker;
import org.jlab.clas.std.orchestrators.ReconstructionOrchestrator.DpeCallBack;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;
import org.jlab.clas.std.orchestrators.errors.OrchestratorConfigError;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

public final class CloudOrchestrator {

    private final ReconstructionOrchestrator orchestrator;

    private final CloudSetup setup;
    private final CloudPaths paths;
    private final CloudStats stats;

    private final Set<ReconstructionNode> nodes;
    private final BlockingQueue<ReconstructionNode> freeNodes;
    private final ExecutorService nodesExecutor;

    private final BlockingQueue<String> processingQueue = new LinkedBlockingQueue<String>();
    private final AtomicInteger processedFilesCounter = new AtomicInteger();

    private final Semaphore recSem;
    private volatile boolean recStat;
    private volatile String recMsg = "Could not run reconstruction!";

    public static void main(String[] args) {
        try {
            CloudOrchestrator fo;
            ConfigFileChecker cc = new ConfigFileChecker(args);
            if (cc.hasFile()) {
                ConfigFileBuilder cf = new ConfigFileBuilder(cc.getFile());
                fo = cf.build();
            } else {
                CommandLineBuilder cl = new CommandLineBuilder(args);
                if (!cl.success()) {
                    System.err.printf("Usage:%n%n  farm-orchestrator %s%n%n%n", cl.usage());
                    System.err.print(cl.help());
                    System.exit(1);
                }
                fo = cl.build();
            }
            boolean status = fo.run();
            if (status) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        } catch (OrchestratorConfigError e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }


    /**
     * Helps constructing a {@link CloudOrchestrator} with all default and
     * required parameters.
     */
    public static class Builder {

        final List<ServiceInfo> recChain;
        final List<String> inputFiles;

        private String frontEnd = "localhost";
        private boolean useFrontEnd = false;

        private int maxThreads = CloudSetup.MAX_THREADS;
        private int maxNodes = CloudSetup.MAX_NODES;

        private String inputDir = CloudPaths.INPUT_DIR;
        private String outputDir = CloudPaths.OUTPUT_DIR;
        private String stageDir = CloudPaths.STAGE_DIR;

        /**
         * Sets the required arguments to start a reconstruction.
         *
         * @param servicesFile the YAML file describing the reconstruction chain
         * @param inputFiles the list of files to be processed (only names).
         * @throws OrchestratorConfigError if the reconstruction chain could not be parsed
         */
        public Builder(String servicesFile, List<String> inputFiles) {
            Objects.requireNonNull(servicesFile, "servicesFile parameter is null");
            Objects.requireNonNull(inputFiles, "inputFiles parameter is null");
            if (inputFiles.isEmpty()) {
                throw new IllegalArgumentException("inputFiles list is empty");
            }
            ReconstructionConfigParser parser = new ReconstructionConfigParser(servicesFile);
            this.recChain = parser.parseReconstructionChain();
            this.inputFiles = inputFiles;
        }

        /**
         * Sets the host of the front-end. Use this if the orchestrator is not
         * running in the same node as the front-end, or if the orchestrator is
         * not using the proper network interface for the front-end.
         */
        public Builder withFrontEnd(String frontEnd) {
            Objects.requireNonNull(frontEnd, "frontEnd parameter is null");
            if (frontEnd.isEmpty()) {
                throw new IllegalArgumentException("frontEnd parameter is empty");
            }
            this.frontEnd = frontEnd;
            return this;
        }

        /**
         * Uses the front-end for reconstruction. By default, the front-end is
         * only used for registration and discovery.
         */
        public Builder useFrontEnd() {
            this.useFrontEnd = true;
            return this;
        }

        /**
         * Sets the maximum number of threads to be used for reconstruction on
         * every node.
         */
        public Builder withMaxThreads(int maxThreads) {
            if (maxThreads <= 0) {
                throw new IllegalArgumentException("Invalid max number of threads: " + maxThreads);
            }
            this.maxThreads = maxThreads;
            return this;
        }

        /**
         * Sets the maximum number of nodes to be used for reconstruction.
         */
        public Builder withMaxNodes(int maxNodes) {
            if (maxNodes <= 0) {
                throw new IllegalArgumentException("Invalid max number of nodes: " + maxNodes);
            }
            this.maxNodes = maxNodes;
            return this;
        }

        /**
         * Changes the path of the shared input directory.
         * This directory should contain all input files.
         */
        public Builder withInputDirectory(String inputDir) {
            Objects.requireNonNull(inputDir, "inputDir parameter is null");
            if (inputDir.isEmpty()) {
                throw new IllegalArgumentException("inputDir parameter is empty");
            }
            this.inputDir = inputDir;
            return this;
        }

        /**
         * Changes the path of the shared output directory.
         * This directory will contain all reconstructed output files.
         */
        public Builder withOutputDirectory(String outputDir) {
            Objects.requireNonNull(outputDir, "outputDir parameter is null");
            if (outputDir.isEmpty()) {
                throw new IllegalArgumentException("outputDir parameter is empty");
            }
            this.outputDir = outputDir;
            return this;
        }

        /**
         * Changes the path of the local staging directory.
         * Files will be staged in this directory of every worker node
         * for fast access.
         */
        public Builder withStageDirectory(String stageDir) {
            Objects.requireNonNull(stageDir, "stageDir parameter is null");
            if (stageDir.isEmpty()) {
                throw new IllegalArgumentException("stageDir parameter is empty");
            }
            this.stageDir = stageDir;
            return this;
        }

        /**
         * Creates the orchestrator.
         */
        public CloudOrchestrator build() {
            CloudSetup setup = new CloudSetup(recChain, "localhost", frontEnd,
                                              useFrontEnd, maxNodes, maxThreads);
            CloudPaths paths = new CloudPaths(inputFiles, inputDir, outputDir, stageDir);
            return new CloudOrchestrator(setup, paths);
        }
    }


    private static class CloudSetup {

        final String localHost;
        final String frontEnd;
        final List<ServiceInfo> recChain;
        final boolean useFrontEnd;
        final int maxNodes;
        final int maxThreads;

        static final int MAX_NODES = 512;
        static final int MAX_THREADS = 64;

        CloudSetup(List<ServiceInfo> recChain,
                   String localhost,
                   String frontEnd,
                   boolean useFrontEnd,
                   int maxNodes,
                   int maxThreads) {
            this.localHost = ReconstructionConfigParser.hostAddress(localhost);
            this.frontEnd = ReconstructionConfigParser.hostAddress(frontEnd);
            this.recChain = recChain;
            this.useFrontEnd = useFrontEnd;
            this.maxNodes = maxNodes;
            this.maxThreads = maxThreads;
        }

        CloudSetup(String frontEnd, List<ServiceInfo> recChain) {
            this(recChain, "localhost", frontEnd, true, MAX_NODES, MAX_THREADS);
        }
    }


    private static class CloudPaths {

        static final String DATA_DIR = System.getenv("CLARA_HOME") + File.separator + "data";
        static final String CACHE_DIR = "/mss/hallb/exp/raw";
        static final String INPUT_DIR = DATA_DIR + File.separator + "in";
        static final String OUTPUT_DIR = DATA_DIR + File.separator + "out";
        static final String STAGE_DIR = File.separator + "scratch";

        final String inputDir;
        final String outputDir;
        final String stageDir;

        final List<String> inputFiles;
        final BlockingQueue<String> requestedFiles;

        CloudPaths(List<String> inputFiles,
                   String inputDir,
                   String outputDir,
                   String stageDir) {
            this.inputFiles = inputFiles;
            this.inputDir = inputDir;
            this.outputDir = outputDir;
            this.stageDir = stageDir;

            this.requestedFiles = new LinkedBlockingDeque<String>();
            for (String name : inputFiles) {
                this.requestedFiles.add(inputDir + File.separator + name);
            }
        }
    }


    private static class CloudStats {

        private final Map<ReconstructionNode, NodeStats> recStats = new ConcurrentHashMap<>();
        private AtomicInteger events = new AtomicInteger();
        private AtomicLong startTime = new AtomicLong();
        private AtomicLong endTime = new AtomicLong();

        private static class NodeStats {
            private int events = 0;
            private long totalTime = 0;
        }

        public void add(ReconstructionNode node) {
            recStats.put(node, new NodeStats());
        }

        public void startClock() {
            startTime.compareAndSet(0, System.currentTimeMillis());
        }

        public void stopClock() {
            endTime.set(System.currentTimeMillis());
        }

        public void update(ReconstructionNode node, int eventNumber, long recTime) {
            NodeStats nodeStats = recStats.get(node);
            nodeStats.events += eventNumber;
            nodeStats.totalTime += recTime;
            events.addAndGet(eventNumber);
        }

        public double localAverage() {
            double avgSum = 0;
            int avgCount = 0;
            for (Entry<ReconstructionNode, NodeStats> entry : recStats.entrySet()) {
                NodeStats stat = entry.getValue();
                if (stat.events > 0) {
                    avgSum += stat.totalTime / (double) stat.events;
                    avgCount++;
                }
            }
            return avgSum / avgCount;
        }

        public double globalAverage() {
            double recTime = endTime.get() - startTime.get();
            return recTime / events.get();
        }
    }


    private CloudOrchestrator(CloudSetup setup, CloudPaths paths) {
        try {
            orchestrator = new ReconstructionOrchestrator(setup.frontEnd, 2);
            orchestrator.setReconstructionChain(setup.recChain);

            this.setup = setup;
            this.paths = paths;
            this.nodes = new HashSet<>();
            this.freeNodes = new LinkedBlockingQueue<>();

            this.nodesExecutor = Executors.newCachedThreadPool();
            this.recSem = new Semaphore(1);
            this.stats = new CloudStats();

            Logging.verbose(true);
        } catch (ClaraException | IOException e) {
            throw new OrchestratorError("Could not connect to Clara", e);
        }
    }


    /**
     * Runs the reconstruction.
     *
     * @return status of the reconstruction.
     * @throws OrchestratorError in case of any error that aborted the reconstruction
     */
    public boolean run() {
        try {
            printStartup();
            Logging.info("Waiting for reconstruction nodes...");
            orchestrator.listenDpes(new DpeReportCB());
            Logging.info("Monitoring files on input directory...");
            new Thread(new FileMonitoringWorker(), "file-monitoring-thread").start();
            startRec();
            waitRec();
            end();
            return recStat;
        } catch (OrchestratorError e) {
            // cleanup();
            end();
            throw e;
        }
    }


    private void startRec() {
        try {
            recSem.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("Could not block processing.");
        }
        try {
            processAllFiles();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted");
        }
    }


    private void waitRec() {
        try {
            recSem.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            recMsg = "Processing interrupted...";
        }
    }


    private void exitRec(boolean status, String msg) {
        recStat = status;
        recMsg = msg;
        recSem.release();
    }


    private void end() {
        Logging.info("Local  average event processing time = %.2f ms", stats.localAverage());
        Logging.info("Global average event processing time = %.2f ms", stats.globalAverage());

        nodesExecutor.shutdown();
        Logging.info(recMsg);
    }


    private void printStartup() {
        System.out.println("****************************************");
        System.out.println("*        CLAS Cloud Orchestrator       *");
        System.out.println("****************************************");
        System.out.println("- Host         = " + setup.localHost);
        System.out.println("- Front-end    = " + setup.frontEnd);
        System.out.println("- Start time   = " + ClaraUtil.getCurrentTimeInH());
        System.out.println();
        System.out.println("- Input directory  = " + paths.inputDir);
        System.out.println("- Output directory = " + paths.outputDir);
        System.out.println("- Stage directory  = " + paths.stageDir);
        System.out.println("- Number of files  = " + paths.inputFiles.size());
        System.out.println("****************************************");
    }



    private class DpeReportCB implements DpeCallBack {

        @Override
        public void callback(DpeInfo dpe) {
            final ReconstructionNode node = new ReconstructionNode(orchestrator, dpe);
            synchronized (nodes) {
                if (nodes.size() == setup.maxNodes || filterNode(node)) {
                    return;
                }
                if (!nodes.contains(node)) {
                    nodes.add(node);
                    stats.add(node);
                    nodesExecutor.execute(() -> setupNode(node));
                }
            }
        }

        private boolean filterNode(ReconstructionNode node) {
            String ip = node.dpe.name.address().host();
            if (ip.equals(setup.frontEnd) && !setup.useFrontEnd) {
                return true;
            }
            return false;
        }
    }


    private void setupNode(ReconstructionNode node) {
        try {
            Logging.info("Start processing on " + node.dpe.name + "...");
            orchestrator.deployInputOutputServices(node.dpe, 1);
            int cores = node.dpe.cores;
            if (cores > 12) {
                cores = 12;
            }
            orchestrator.deployReconstructionChain(node.dpe, cores);
            orchestrator.checkInputOutputServices(node.dpe);
            orchestrator.checkReconstructionServices(node.dpe);
            orchestrator.subscribeErrors(node.containerName, new ErrorHandlerCB(node));
            Logging.info("All services deployed on " + node.dpe.name);
            freeNodes.add(node);
        } catch (OrchestratorError e) {
            Logging.error("Could not use %s for reconstruction%n%s",
                          node.dpe.name, e.getMessage());
        }
    }


    private class FileMonitoringWorker implements Runnable {

        @Override
        public void run() {
            while (!paths.requestedFiles.isEmpty()) {
                Path filePath = Paths.get(paths.requestedFiles.element());
                if (filePath.toFile().exists()) {
                    Path fileName = filePath.getFileName();
                    if (fileName == null) {
                        Logging.error("Empty file path");
                        paths.requestedFiles.remove();
                        continue;
                    }
                    processingQueue.add(fileName.toString());
                    paths.requestedFiles.remove();
                    Logging.info(filePath + " is cached.");
                }
                orchestrator.sleep(100);
            }
        }
    }


    private void processAllFiles() throws InterruptedException {
        if (setup.maxNodes < CloudSetup.MAX_NODES) {
            while (freeNodes.size() < setup.maxNodes) {
                orchestrator.sleep(100);
            }
        }
        while (processedFilesCounter.get() < paths.inputFiles.size()) {
            String filePath = processingQueue.peek();
            if (filePath == null) {
                orchestrator.sleep(100);
                continue;
            }
            // TODO check if file exists
            final String fileName = new File(filePath).getName();

            final ReconstructionNode node = freeNodes.poll(60, TimeUnit.SECONDS);
            if (node != null) {
                nodesExecutor.execute(() -> processFile(node, fileName));
                processingQueue.remove();
            }
        }
    }


    private void processFile(ReconstructionNode node, String inputFile) {
        stats.startClock();
        DpeInfo dpe = node.dpe;
        List<ServiceName> recChain = orchestrator.generateReconstructionChain(dpe);

        // TODO check DPE is alive
        node.setPaths(paths.inputDir, paths.outputDir, paths.stageDir);
        node.setFiles(inputFile);
        node.openFiles();

        // TODO send proper configuration data
        EngineData configData = new EngineData();
        configData.setData(EngineDataType.STRING.mimeType(), node.currentInputFile);
        for (ServiceName recService : recChain) {
            node.configureService(recService, configData);
        }

        int threads = dpe.cores <= setup.maxThreads ? dpe.cores : setup.maxThreads;
        node.sendEventsToDpe(dpe.name, recChain, threads);
    }


    private void printAverage(ReconstructionNode node) {
        long endTime = System.currentTimeMillis();
        long recTime = endTime - node.startTime;
        double timePerEvent = recTime / (double) node.eventNumber;
        stats.update(node, node.eventNumber, recTime);
        Logging.info("File %s reconstructed! Average event processing time = %.2f ms",
                     node.currentInputFileName, timePerEvent);
    }


    private void processFinishedFile(ReconstructionNode node) {
        node.closeFiles();
        int counter = processedFilesCounter.incrementAndGet();
        if (counter == paths.inputFiles.size()) {
            stats.stopClock();
            exitRec(true, "Processing is complete.");
        } else {
            freeNodes.add(node);
        }
    }


    private class ErrorHandlerCB implements EngineCallback {

        private final ReconstructionNode node;

        ErrorHandlerCB(ReconstructionNode node) {
            this.node = node;
        }

        @Override
        public void callback(EngineData data) {
            try {
                handleError(data);
            } catch (OrchestratorError e) {
                Logging.error("Could not use %s for reconstruction%n%s",
                              node.dpe.name, e.getMessage());
            }
        }

        private synchronized void handleError(EngineData data) {
            ServiceName source = new ServiceName(data.getEngineName());
            DpeName host = source.dpe();
            int requestId = data.getCommunicationId();
            int severity = data.getStatusSeverity();
            String description = data.getDescription();
            if (description.equalsIgnoreCase("End of File")) {
                if (severity == 2) {
                    printAverage(node);
                    processFinishedFile(node);
                }
            } else if (description.startsWith("Error opening the file")) {
                Logging.error(description);
            } else {
                Logging.error("Error in %s (ID: %d):%n%s", source, requestId, description);
                List<ServiceName> chain = orchestrator.generateReconstructionChain(node.dpe);
                node.requestEvent(host, chain, requestId, "next-rec");
            }
        }
    }


    public static class CommandLineBuilder {

        private static final String ARG_FRONTEND      = "frontEnd";
        private static final String ARG_USE_FRONTEND  = "useFrontEnd";
        private static final String ARG_CACHE_DIR     = "cacheDir";
        private static final String ARG_INPUT_DIR     = "inputDir";
        private static final String ARG_OUTPUT_DIR    = "outputDir";
        private static final String ARG_STAGE_DIR     = "stageDir";
        private static final String ARG_MAX_NODES     = "maxNodes";
        private static final String ARG_MAX_THREADS   = "maxThreads";
        private static final String ARG_SERVICES_FILE = "servicesFile";
        private static final String ARG_INPUT_FILES   = "inputFiles";

        private final JSAP jsap;
        private final JSAPResult config;

        public CommandLineBuilder(String[] args) {
            jsap = new JSAP();
            setArguments(jsap);
            config = jsap.parse(args);
        }

        public boolean success() {
            return config.success();
        }

        public String usage() {
            return jsap.getUsage();
        }

        public String help() {
            return jsap.getHelp();
        }

        public CloudOrchestrator build() {
            String frontEnd = config.getString(ARG_FRONTEND);
            boolean useFrontEnd = config.getBoolean(ARG_USE_FRONTEND);
            int maxNodes = config.getInt(ARG_MAX_NODES);
            int maxThreads = config.getInt(ARG_MAX_THREADS);

            String services = config.getString(ARG_SERVICES_FILE);
            String files = config.getString(ARG_INPUT_FILES);

            String inDir = config.getString(ARG_INPUT_DIR);
            String outDir = config.getString(ARG_OUTPUT_DIR);
            String tmpDir = config.getString(ARG_STAGE_DIR);

            ReconstructionConfigParser parser = new ReconstructionConfigParser(services);
            List<ServiceInfo> recChain = parser.parseReconstructionChain();
            List<String> inFiles = parser.readInputFiles(files);

            CloudSetup setup = new CloudSetup(recChain, "localhost", frontEnd,
                                              useFrontEnd, maxNodes, maxThreads);
            CloudPaths paths = new CloudPaths(inFiles, inDir, outDir, tmpDir);

            return new CloudOrchestrator(setup, paths);
        }

        private void setArguments(JSAP jsap) {
            FlaggedOption frontEnd = new FlaggedOption(ARG_FRONTEND)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('f')
                    .setDefault("localhost");
            frontEnd.setHelp("The address of the CLARA front-end.");

            Switch useFrontEnd = new Switch(ARG_USE_FRONTEND)
                    .setShortFlag('F');
            useFrontEnd.setHelp("Use front-end for reconstruction");

            FlaggedOption tapeDir = new FlaggedOption(ARG_CACHE_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('c')
                    .setDefault(CloudPaths.CACHE_DIR);
            tapeDir.setHelp("The tape directory where from files are cached.");

            FlaggedOption inputDir = new FlaggedOption(ARG_INPUT_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('i')
                    .setDefault(CloudPaths.INPUT_DIR);
            inputDir.setHelp("The input directory where the files to be processed are located.");

            FlaggedOption outputDir = new FlaggedOption(ARG_OUTPUT_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('o')
                    .setDefault(CloudPaths.OUTPUT_DIR);
            outputDir.setHelp("The output directory where reconstructed files will be saved.");

            FlaggedOption stageDir = new FlaggedOption(ARG_STAGE_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('s')
                    .setDefault(CloudPaths.STAGE_DIR);
            stageDir.setHelp("The stage directory where the local temporary files will be stored.");

            FlaggedOption maxNodes = new FlaggedOption(ARG_MAX_NODES)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('n')
                    .setDefault(String.valueOf(CloudSetup.MAX_NODES))
                    .setRequired(false);
            maxNodes.setHelp("The maximum number of reconstruction nodes to be used.");

            FlaggedOption maxThreads = new FlaggedOption(ARG_MAX_THREADS)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('t')
                    .setDefault(String.valueOf(CloudSetup.MAX_THREADS))
                    .setRequired(false);
            maxThreads.setHelp("The maximum number of reconstruction threads to be used per node.");

            UnflaggedOption servicesFile = new UnflaggedOption(ARG_SERVICES_FILE)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true);
            servicesFile.setHelp("The YAML file with the reconstruction chain description.");

            UnflaggedOption inputFiles = new UnflaggedOption(ARG_INPUT_FILES)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true);
            inputFiles.setHelp("The file with the list of input files to be reconstructed"
                              + " (one name per line).");

            try {
                jsap.registerParameter(frontEnd);
                jsap.registerParameter(useFrontEnd);
                jsap.registerParameter(inputDir);
                jsap.registerParameter(outputDir);
                jsap.registerParameter(stageDir);
                jsap.registerParameter(maxNodes);
                jsap.registerParameter(maxThreads);
                jsap.registerParameter(servicesFile);
                jsap.registerParameter(inputFiles);
            } catch (JSAPException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static class ConfigFileBuilder {

        private final String configFile;

        public ConfigFileBuilder(String configFile) {
            this.configFile = configFile;
        }

        public CloudOrchestrator build() {
            ReconstructionConfigParser parser = new ReconstructionConfigParser(configFile);

            String frontEnd = "localhost";
            List<ServiceInfo> recChain = parser.parseReconstructionChain();
            List<String> inFiles = parser.readInputFiles();

            CloudSetup setup = new CloudSetup(frontEnd, recChain);
            CloudPaths paths = new CloudPaths(inFiles,
                                              parser.parseDirectory("input"),
                                              parser.parseDirectory("output"),
                                              parser.parseDirectory("tmp"));
            return new CloudOrchestrator(setup, paths);
        }
    }
}
