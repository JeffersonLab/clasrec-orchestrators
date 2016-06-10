package org.jlab.clas.std.orchestrators;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;


abstract class AbstractOrchestrator {

    final ReconstructionOrchestrator orchestrator;

    final ReconstructionSetup setup;
    final ReconstructionPaths paths;
    final ReconstructionStats stats;

    private final BlockingQueue<ReconstructionNode> freeNodes;
    private final ExecutorService nodesExecutor;

    private final BlockingQueue<ReconstructionFile> processingQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger processedFilesCounter = new AtomicInteger();

    private final Semaphore recSem;
    private volatile boolean recStatus;
    private volatile String recMsg = "Could not run reconstruction!";

    static class ReconstructionSetup {

        final String localHost;
        final String frontEnd;
        final List<ServiceInfo> recChain;
        final boolean useFrontEnd;
        final boolean stageFiles;
        final int poolSize;
        final int maxNodes;
        final int maxThreads;

        static final int DEFAULT_POOLSIZE = 32;
        static final int MAX_NODES = 512;
        static final int MAX_THREADS = 64;

        // CHECKSTYLE.OFF: ParameterNumber
        ReconstructionSetup(List<ServiceInfo> recChain,
                            String localhost,
                            String frontEnd,
                            boolean useFrontEnd,
                            boolean stageFiles,
                            int poolSize,
                            int maxNodes,
                            int maxThreads) {
            this.localHost = ReconstructionConfigParser.hostAddress(localhost);
            this.frontEnd = ReconstructionConfigParser.hostAddress(frontEnd);
            this.recChain = recChain;
            this.useFrontEnd = useFrontEnd;
            this.stageFiles = stageFiles;
            this.poolSize = poolSize;
            this.maxNodes = maxNodes;
            this.maxThreads = maxThreads;
        }
        // CHECKSTYLE.ON: ParameterNumber

        ReconstructionSetup(String frontEnd, List<ServiceInfo> recChain) {
            this(recChain, "localhost", frontEnd, true, true,
                 DEFAULT_POOLSIZE, MAX_NODES, MAX_THREADS);
        }
    }


    static class ReconstructionFile {

        final String inputName;
        final String outputName;

        ReconstructionFile(String inFile, String outFile) {
            inputName = inFile;
            outputName = outFile;
        }
    }


    static class ReconstructionPaths {

        static final String DATA_DIR = System.getenv("CLARA_HOME") + File.separator + "data";
        static final String CACHE_DIR = "/mss/hallb/exp/raw";
        static final String INPUT_DIR = DATA_DIR + File.separator + "in";
        static final String OUTPUT_DIR = DATA_DIR + File.separator + "out";
        static final String STAGE_DIR = File.separator + "scratch";

        final String inputDir;
        final String outputDir;
        final String stageDir;

        private final List<ReconstructionFile> allFiles;

        ReconstructionPaths(List<String> inputFiles,
                            String inputDir,
                            String outputDir,
                            String stageDir) {
            this.inputDir = inputDir;
            this.outputDir = outputDir;
            this.stageDir = stageDir;
            this.allFiles = inputFiles.stream()
                                      .map(f -> new ReconstructionFile(f, "out_" + f))
                                      .collect(Collectors.toList());
        }

        String inputFilePath(ReconstructionFile recFile) {
            return inputDir + File.separator + recFile.inputName;
        }

        String outputFilePath(ReconstructionFile recFile) {
            return outputDir + File.separator + recFile.outputName;
        }

        int numFiles() {
            return allFiles.size();
        }
    }


    static class ReconstructionStats {

        private final Map<ReconstructionNode, NodeStats> recStats = new ConcurrentHashMap<>();
        private AtomicInteger events = new AtomicInteger();
        private AtomicLong startTime = new AtomicLong();
        private AtomicLong endTime = new AtomicLong();

        private static class NodeStats {
            private int events = 0;
            private long totalTime = 0;
        }

        void add(ReconstructionNode node) {
            recStats.put(node, new NodeStats());
        }

        void startClock() {
            startTime.compareAndSet(0, System.currentTimeMillis());
        }

        void stopClock() {
            endTime.set(System.currentTimeMillis());
        }

        void update(ReconstructionNode node, int recEvents, long recTime) {
            NodeStats nodeStats = recStats.get(node);
            nodeStats.events += recEvents;
            nodeStats.totalTime += recTime;
            events.addAndGet(recEvents);
        }

        double localAverage() {
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

        double globalAverage() {
            double recTime = endTime.get() - startTime.get();
            return recTime / events.get();
        }
    }


    AbstractOrchestrator(ReconstructionSetup setup, ReconstructionPaths paths) {
        try {
            orchestrator = new ReconstructionOrchestrator(setup.frontEnd, setup.poolSize);
            orchestrator.setReconstructionChain(setup.recChain);

            this.setup = setup;
            this.paths = paths;

            this.freeNodes = new LinkedBlockingQueue<>();
            this.nodesExecutor = Executors.newCachedThreadPool();

            this.recSem = new Semaphore(1);
            this.stats = new ReconstructionStats();

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
            start();
            checkFiles();
            startRec();
            waitRec();
            end();
            destroy();
            return recStatus;
        } catch (OrchestratorError e) {
            // cleanup();
            destroy();
            throw e;
        }
    }


    abstract void start();

    abstract void end();


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


    void exitRec(boolean status, String msg) {
        recStatus = status;
        recMsg = msg;
        recSem.release();
    }


    void destroy() {
        nodesExecutor.shutdown();
        Logging.info(recMsg);
    }


    /**
     * @throws RejectedExecutionException
     */
    void executeSetup(ReconstructionNode node) {
        nodesExecutor.execute(() -> {
            try {
                setupNode(node);
            } catch (OrchestratorError e) {
                Logging.error("Could not use %s for reconstruction%n%s",
                              node.dpe.name, e.getMessage());
            }
        });
    }


    /**
     * @throw OrchestratorError
     */
    void setupNode(ReconstructionNode node) {
        try {
            Logging.info("Start processing on %s...", node.dpe.name);
            if (!checkChain(node)) {
                deploy(node);
            }
            subscribe(node);

            Logging.info("Configuring services on %s...", node.dpe.name);
            if (setup.stageFiles) {
                node.setPaths(paths.inputDir, paths.outputDir, paths.stageDir);
            }
            // TODO send proper configuration data
            EngineData configData = new EngineData();
            configData.setData(EngineDataType.STRING.mimeType(), "config");
            List<ServiceName> recChain = orchestrator.generateReconstructionChain(node.dpe);
            for (ServiceName recService : recChain) {
                node.configureService(recService, configData);
            }
            Logging.info("All services configured on %s", node.dpe.name);

            freeNodes.add(node);
            stats.add(node);
        } catch (OrchestratorError e) {
            // TODO cleanup
            throw e;
        }
    }


    boolean checkChain(ReconstructionNode node) {
        Logging.info("Searching services in %s...", node.dpe.name);
        if (!orchestrator.findInputOutputService(node.dpe)) {
            return false;
        }
        if (!orchestrator.findReconstructionServices(node.dpe)) {
            return false;
        }
        Logging.info("All services already deployed on %s", node.dpe.name);
        return true;
    }


    void deploy(ReconstructionNode node) {
        Logging.info("Deploying services in %s...", node.dpe.name);
        orchestrator.deployInputOutputServices(node.dpe, 1);
        orchestrator.deployReconstructionChain(node.dpe, node.dpe.cores);
        orchestrator.checkInputOutputServices(node.dpe);
        orchestrator.checkReconstructionServices(node.dpe);
        Logging.info("All services deployed on %s", node.dpe.name);
    }


    void subscribe(ReconstructionNode node) {
        orchestrator.subscribeErrors(node.containerName, new ErrorHandlerCB(node));
    }


    private void checkFiles() {
        if (setup.stageFiles) {
            Logging.info("Monitoring files on input directory...");
            new Thread(new FileMonitoringWorker(), "file-monitoring-thread").start();
        } else {
            for (ReconstructionFile file : paths.allFiles) {
                processingQueue.add(file);
            }
        }
    }


    private class FileMonitoringWorker implements Runnable {

        @Override
        public void run() {
            BlockingQueue<ReconstructionFile> requestedFiles = new LinkedBlockingDeque<>();
            for (ReconstructionFile file : paths.allFiles) {
                requestedFiles.add(file);
            }
            while (!requestedFiles.isEmpty()) {
                ReconstructionFile recFile = requestedFiles.element();
                Path filePath = Paths.get(paths.inputFilePath(recFile));
                if (filePath.toFile().exists()) {
                    processingQueue.add(recFile);
                    requestedFiles.remove();
                    Logging.info("File %s is cached", filePath);
                } else {
                    orchestrator.sleep(100);
                }
            }
        }
    }


    private void processAllFiles() throws InterruptedException {
        if (setup.maxNodes < ReconstructionSetup.MAX_NODES) {
            while (freeNodes.size() < setup.maxNodes) {
                orchestrator.sleep(100);
            }
        }
        while (processedFilesCounter.get() < paths.numFiles()) {
            ReconstructionFile recFile = processingQueue.peek();
            if (recFile == null) {
                orchestrator.sleep(100);
                continue;
            }
            // TODO check if file exists
            final ReconstructionNode node = freeNodes.poll(60, TimeUnit.SECONDS);
            if (node != null) {
                try {
                    nodesExecutor.execute(() -> processFile(node, recFile));
                    processingQueue.remove();
                } catch (RejectedExecutionException e) {
                    freeNodes.add(node);
                }
            }
        }
    }


    void processFile(ReconstructionNode node, ReconstructionFile recFile) {
        try {
            stats.startClock();
            // TODO check DPE is alive
            openFiles(node, recFile);
            startFile(node);
        } catch (OrchestratorError e) {
            Logging.error("Could not use %s for reconstruction%n%s", node.dpe.name, e.getMessage());
        }
    }


    void openFiles(ReconstructionNode node, ReconstructionFile recFile) {
        if (setup.stageFiles) {
            node.setFiles(recFile.inputName);
        } else {
            node.setFiles(paths.inputFilePath(recFile), paths.outputFilePath(recFile));
        }
        node.openFiles();
    }


    void startFile(ReconstructionNode node) {
        int fileCounter = processedFilesCounter.get() + 1;
        int totalFiles = paths.numFiles();
        node.setFileCounter(fileCounter, totalFiles);

        List<ServiceName> recChain = orchestrator.generateReconstructionChain(node.dpe);
        int threads = node.dpe.cores <= setup.maxThreads ? node.dpe.cores : setup.maxThreads;
        node.sendEventsToDpe(node.dpe.name, recChain, threads);
    }


    void printAverage(ReconstructionNode node) {
        long endTime = System.currentTimeMillis();
        long recTime = endTime - node.startTime;
        double timePerEvent = recTime / (double) node.totalEvents;
        stats.update(node, node.totalEvents, recTime);
        Logging.info("Finished file %s on %s. Average event time = %.2f ms",
                     node.currentInputFileName, node.dpe.name, timePerEvent);
    }


    void processFinishedFile(ReconstructionNode node) {
        try {
            String currentFile = node.currentInputFileName;
            node.closeFiles();
            if (setup.stageFiles) {
                node.saveOutputFile();
                Logging.info("Saved file %s on %s", currentFile, node.dpe.name);
            }
        } catch (OrchestratorError e) {
            Logging.error("Could not close files on %s%n%s", node.dpe.name, e.getMessage());
        } finally {
            int counter = processedFilesCounter.incrementAndGet();
            if (counter == paths.numFiles()) {
                stats.stopClock();
                exitRec(true, "Processing is complete.");
            } else {
                freeNodes.add(node);
            }
        }
    }


    private class ErrorHandlerCB implements EngineCallback {

        private final ReconstructionNode node;

        ErrorHandlerCB(ReconstructionNode node) {
            this.node = node;
        }

        @Override
        public void callback(EngineData data) {
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
                try {
                    Logging.error("Error in %s (ID: %d):%n%s", source, requestId, description);
                    List<ServiceName> chain = orchestrator.generateReconstructionChain(node.dpe);
                    node.requestEvent(host, chain, requestId, "next-rec");
                } catch (OrchestratorError e) {
                    Logging.error(e.getMessage());
                }
            }
        }
    }
}
