package org.jlab.clas.std.orchestrators;

import java.nio.file.Path;
import java.nio.file.Paths;
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

import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;


abstract class AbstractOrchestrator {

    final ReconstructionOrchestrator orchestrator;

    final ReconstructionSetup setup;
    final ReconstructionPaths paths;
    final ReconstructionOptions options;
    final ReconstructionStats stats;

    private final BlockingQueue<ReconstructionNode> freeNodes;
    private final ExecutorService nodesExecutor;

    private final BlockingQueue<ReconstructionFile> processingQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<ReconstructionFile> finishedQueue = new LinkedBlockingQueue<>();

    private final AtomicInteger startedFilesCounter = new AtomicInteger();
    private final AtomicInteger processedFilesCounter = new AtomicInteger();

    private final Semaphore recSem;
    private volatile boolean recStatus;
    private volatile String recMsg = "Could not run reconstruction!";

    static class ReconstructionOptions {

        final boolean useFrontEnd;
        final boolean stageFiles;
        final boolean bulkStage;
        final int poolSize;
        final int maxNodes;
        final int maxThreads;
        final int reportFreq;

        static final int DEFAULT_POOLSIZE = 32;
        static final int MAX_NODES = 512;
        static final int MAX_THREADS = 64;

        /**
         * Default options.
         */
        ReconstructionOptions() {
            this(true, true, false, DEFAULT_POOLSIZE, MAX_NODES, MAX_THREADS);
        }

        /**
         * LocalOrchestrator options.
         */
        ReconstructionOptions(boolean stageFiles,
                              int poolSize,
                              int maxThreads,
                              int reportFreq) {
            this.useFrontEnd = true;
            this.stageFiles = stageFiles;
            this.bulkStage = false;
            this.poolSize = poolSize;
            this.maxNodes = 1;
            this.maxThreads = maxThreads;
            this.reportFreq = reportFreq;
        }

        /**
         * CloudOrchestrator options.
         */
        ReconstructionOptions(boolean useFrontEnd,
                              boolean stageFiles,
                              boolean bulkStage,
                              int poolSize,
                              int maxNodes,
                              int maxThreads) {
            this.useFrontEnd = useFrontEnd;
            this.stageFiles = stageFiles || bulkStage;
            this.bulkStage = bulkStage;
            this.poolSize = poolSize;
            this.maxNodes = maxNodes;
            this.maxThreads = maxThreads;
            this.reportFreq = 0;
        }
    }


    static class ReconstructionStats {

        private final Map<ReconstructionNode, NodeStats> recStats = new ConcurrentHashMap<>();
        private final AtomicLong startTime = new AtomicLong();
        private final AtomicLong endTime = new AtomicLong();

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
            synchronized (nodeStats) {
                nodeStats.events += recEvents;
                nodeStats.totalTime += recTime;
            }
        }

        long totalEvents() {
            long sum = 0;
            for (Entry<ReconstructionNode, NodeStats> entry : recStats.entrySet()) {
                NodeStats stat = entry.getValue();
                synchronized (stat) {
                    if (stat.events > 0) {
                        sum += stat.events;
                    }
                }
            }
            return sum;
        }

        long totalTime() {
            long sum = 0;
            for (Entry<ReconstructionNode, NodeStats> entry : recStats.entrySet()) {
                NodeStats stat = entry.getValue();
                synchronized (stat) {
                    if (stat.events > 0) {
                        sum += stat.totalTime;
                    }
                }
            }
            return sum;
        }

        double globalTime() {
            return endTime.get() - startTime.get();
        }

        double localAverage() {
            double avgSum = 0;
            int avgCount = 0;
            for (Entry<ReconstructionNode, NodeStats> entry : recStats.entrySet()) {
                NodeStats stat = entry.getValue();
                synchronized (stat) {
                    if (stat.events > 0) {
                        avgSum += stat.totalTime / (double) stat.events;
                        avgCount++;
                    }
                }
            }
            return avgSum / avgCount;
        }

        double globalAverage() {
            return globalTime() / totalEvents();
        }
    }


    AbstractOrchestrator(ReconstructionSetup setup,
                         ReconstructionPaths paths,
                         ReconstructionOptions options) {
        this.orchestrator = new ReconstructionOrchestrator(setup, options.poolSize);

        this.setup = setup;
        this.paths = paths;
        this.options = options;

        this.freeNodes = new LinkedBlockingQueue<>();
        this.nodesExecutor = Executors.newCachedThreadPool();

        this.recSem = new Semaphore(1);
        this.stats = new ReconstructionStats();
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


    ReconstructionNode localNode() {
        int cores = Runtime.getRuntime().availableProcessors();
        DpeInfo dpe = new DpeInfo(orchestrator.getFrontEnd(), cores, DpeInfo.DEFAULT_CLARA_HOME);
        ReconstructionApplication app = new ReconstructionApplication(setup.application, dpe);
        return new ReconstructionNode(orchestrator, app);
    }


    /**
     * @throws RejectedExecutionException
     */
    void executeSetup(ReconstructionNode node) {
        nodesExecutor.execute(() -> {
            try {
                setupNode(node);
            } catch (OrchestratorError e) {
                Logging.error("Could not use %s for reconstruction:%n%s",
                              node.name(), e.getMessage());
            }
        });
    }


    /**
     * @throw OrchestratorError
     */
    void setupNode(ReconstructionNode node) {
        try {
            Logging.info("Start processing on %s...", node.name());
            if (!checkChain(node)) {
                deploy(node);
            }
            subscribe(node);

            Logging.info("Configuring services on %s...", node.name());
            if (options.stageFiles && !options.bulkStage) {
                node.setPaths(paths.inputDir, paths.outputDir, paths.stageDir);
            }
            // TODO send proper configuration data
            EngineData configData = new EngineData();
            configData.setData(EngineDataType.STRING.mimeType(), "config");
            node.configureServices(configData);
            Logging.info("All services configured on %s", node.name());

            if (options.bulkStage && node.isFrontEnd()) {
                Logging.info("Staging all files on %s...", node.name());
                new Thread(new FileStagingWorker(), "file-staging-thread").start();
                new Thread(new FileSavingWorker(), "file-saving-thread").start();
            }

            freeNodes.add(node);
            stats.add(node);
        } catch (OrchestratorError e) {
            // TODO cleanup
            throw e;
        }
    }


    boolean checkChain(ReconstructionNode node) {
        Logging.info("Searching services in %s...", node.name());
        if (node.checkServices()) {
            Logging.info("All services already deployed on %s", node.name());
            return true;
        }
        return false;
    }


    void deploy(ReconstructionNode node) {
        Logging.info("Deploying services in %s...", node.name());
        node.deployServices();
        Logging.info("All services deployed on %s", node.name());
    }


    void subscribe(ReconstructionNode node) {
        node.subscribeErrors(ErrorHandlerCB::new);
    }


    private void checkFiles() {
        if (options.stageFiles) {
            if (!options.bulkStage) {
                Logging.info("Monitoring files on input directory...");
                new Thread(new FileMonitoringWorker(), "file-monitoring-thread").start();
            }
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


    private class FileStagingWorker implements Runnable {

        @Override
        public void run() {
            BlockingQueue<ReconstructionFile> requestedFiles = new LinkedBlockingDeque<>();
            for (ReconstructionFile file : paths.allFiles) {
                requestedFiles.add(file);
            }

            ReconstructionNode localNode = localNode();
            localNode.setPaths(paths.inputDir, paths.outputDir, paths.stageDir);
            while (!requestedFiles.isEmpty()) {
                ReconstructionFile recFile = requestedFiles.element();
                try {
                    stats.startClock();
                    localNode.setFiles(recFile.inputName);
                    processingQueue.add(recFile);
                    requestedFiles.remove();
                } catch (Exception e) {
                    Logging.error("Could not stage file %s:%n%s",
                            recFile.inputName, e.getMessage());
                }
            }
        }
    }


    private class FileSavingWorker implements Runnable {

        @Override
        public void run() {
            ReconstructionNode localNode = localNode();
            while (true) {
                ReconstructionFile recFile = finishedQueue.peek();
                if (recFile == null) {
                    orchestrator.sleep(100);
                    continue;
                }
                try {
                    localNode.setFiles(paths.stageInputFilePath(recFile),
                                       paths.stageOutputFilePath(recFile));
                    localNode.saveOutputFile();
                    Logging.info("Saved file %s on %s", recFile.outputName, localNode.name());
                } catch (Exception e) {
                    Logging.error("Could not save file %s:%n%s",
                            recFile.outputName, e.getMessage());
                } finally {
                    finishedQueue.remove();
                    if (incrementFinishedFile()) {
                        break;
                    }
                }
            }
        }
    }



    private void processAllFiles() throws InterruptedException {
        if (options.maxNodes < ReconstructionOptions.MAX_NODES) {
            while (freeNodes.size() < options.maxNodes) {
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
            if (!options.bulkStage) {
                stats.startClock();
            }
            // TODO check DPE is alive
            openFiles(node, recFile);
            startFile(node);
        } catch (OrchestratorError e) {
            Logging.error("Could not use %s for reconstruction:%n%s",
                    node.name(), e.getMessage());
        }
    }


    void openFiles(ReconstructionNode node, ReconstructionFile recFile) {
        node.recFile = recFile;
        if (options.stageFiles) {
            if (options.bulkStage) {
                node.setFiles(paths.stageInputFilePath(recFile),
                              paths.stageOutputFilePath(recFile));
            } else {
                node.setFiles(recFile.inputName);
            }
        } else {
            node.setFiles(paths.inputFilePath(recFile), paths.outputFilePath(recFile));
        }
        node.openFiles();
    }


    void startFile(ReconstructionNode node) {
        node.setReportFrequency(options.reportFreq);

        int fileCounter = startedFilesCounter.incrementAndGet();
        int totalFiles = paths.numFiles();
        node.setFileCounter(fileCounter, totalFiles);

        node.sendEvents(options.maxThreads);
    }


    void printAverage(ReconstructionNode node) {
        long endTime = System.currentTimeMillis();
        long recTime = endTime - node.startTime.get();
        double timePerEvent = recTime / (double) node.totalEvents.get();
        stats.update(node, node.totalEvents.get(), recTime);
        Logging.info("Finished file %s on %s. Average event time = %.2f ms",
                     node.currentInputFileName, node.name(), timePerEvent);
    }


    void processFinishedFile(ReconstructionNode node) {
        try {
            String currentFile = node.currentInputFileName;
            node.closeFiles();
            if (options.stageFiles && !options.bulkStage) {
                node.saveOutputFile();
                Logging.info("Saved file %s on %s", currentFile, node.name());
            }
        } catch (OrchestratorError e) {
            Logging.error("Could not close files on %s:%n%s", node.name(), e.getMessage());
        } finally {
            if (options.bulkStage) {
                finishedQueue.add(node.recFile);
            } else {
                incrementFinishedFile();
            }
            freeNodes.add(node);
        }
    }

    private boolean incrementFinishedFile() {
        int counter = processedFilesCounter.incrementAndGet();
        boolean finished = counter == paths.numFiles();
        if (finished) {
            stats.stopClock();
            exitRec(true, "Processing is complete.");
        }
        return finished;
    }


    void removeStageDirectories() {
        if (options.stageFiles) {
            freeNodes.stream().parallel().forEach(n -> {
                try {
                    n.removeStageDir();
                } catch (OrchestratorError e) {
                    Logging.error("Could not remove stage directory from %s: %s",
                            n.name(), e.getMessage());
                }
            });
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
                    node.requestEvent(requestId, "next-rec");
                } catch (OrchestratorError e) {
                    Logging.error(e.getMessage());
                }
            }
        }
    }
}
