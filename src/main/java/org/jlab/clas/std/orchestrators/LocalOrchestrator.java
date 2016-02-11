package org.jlab.clas.std.orchestrators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas.std.orchestrators.ReconstructionConfigParser.ConfigFileChecker;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;
import org.jlab.clas.std.orchestrators.errors.OrchestratorConfigError;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.UnflaggedOption;

/**
 * Deploys and runs the CLAS reconstruction chain on the local node,
 * with the given input file.
 * <p>
 * This orchestrator is called by the {@code local-orchestrator} script.
 */
public class LocalOrchestrator {

    private final ReconstructionOrchestrator orchestrator;
    private final ReconstructionNode ioNode;
    private final Map<DpeName, List<ServiceName>> reconstructionChains = new HashMap<>();

    private final ReconstructionSetup setup;
    private final ReconstructionPaths paths;
    private final ReconstructionOptions options;
    private final ReconstructionStats stats;


    public static void main(String[] args) {
        try {
            LocalOrchestrator dfo;
            ConfigFileChecker cc = new ConfigFileChecker(args);
            if (cc.hasFile()) {
                ConfigFileBuilder cf = new ConfigFileBuilder(cc.getFile());
                dfo = cf.build();
            } else {
                CommandLineBuilder cl = new CommandLineBuilder(args);
                if (!cl.success()) {
                    System.err.printf("Usage:%n%n  local-orchestrator run-chain %s%n%n%n%s",
                                      cl.usage(), cl.help());
                    System.exit(1);
                }
                dfo = cl.build();
            }
            boolean status = dfo.run();
            if (status) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        } catch (OrchestratorConfigError | OrchestratorError e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: Unexpected exception!");
            e.printStackTrace();
            System.exit(1);
        }
    }


    public static class ReconstructionSetup {

        final List<DpeInfo> ioNodes;
        final List<DpeInfo> recNodes;
        final List<ServiceInfo> recChain;

        public ReconstructionSetup(String servicesFile) {
            ioNodes = new ArrayList<DpeInfo>();
            ioNodes.add(ReconstructionConfigParser.getDefaultDpeInfo("localhost"));

            /* Check that all the node names are valid */
            recNodes = new ArrayList<DpeInfo>();
            recNodes.add(ReconstructionConfigParser.getDefaultDpeInfo("localhost"));

            ReconstructionConfigParser parser = new ReconstructionConfigParser(servicesFile);
            recChain = parser.parseReconstructionChain();
        }

        ReconstructionSetup(List<ServiceInfo> recChain,
                            List<DpeInfo> ioNodes,
                            List<DpeInfo> recNodes) {
            this.recChain = recChain;
            this.ioNodes = ioNodes;
            this.recNodes = recNodes;
        }
    }


    public static class ReconstructionPaths {

        final String inputFile;
        final String outputFile;

        public ReconstructionPaths(String inFile, String outFile) {
            inputFile = inFile;
            outputFile = outFile;
        }
    }


    public static class ReconstructionOptions {

        final int threads;
        int reportFreq = 1000;

        public ReconstructionOptions(int numThreads) {
            this.threads = numThreads;
        }

        public void setReportFrequency(int frequency) {
            this.reportFreq = frequency;
        }
    }


    private static class ReconstructionStats {
        private final Semaphore recSem = new Semaphore(1);
        private volatile boolean recStat;
        private volatile String recMsg = "Could not run reconstruction!";

        private long totalTimeStart;
        private long totalTimeEnd;
        private long recTimeStart;
        private long recTimeEnd;
    }



    /**
     * Creates an orchestrator to reconstruct events on the local box.
     *
     * @param setup the description of IO and reconstruction nodes
     * @param paths the location of the input and output files
     * @param opts the reconstruction options
     */
    public LocalOrchestrator(ReconstructionSetup setup,
                             ReconstructionPaths paths,
                             ReconstructionOptions opts) {
        try {
            this.orchestrator = new ReconstructionOrchestrator();
            orchestrator.setReconstructionChain(setup.recChain);
            for (DpeInfo dpe : setup.recNodes) {
                List<ServiceName> chain = orchestrator.generateReconstructionChain(dpe);
                reconstructionChains.put(dpe.name, chain);
            }
            this.ioNode = new ReconstructionNode(orchestrator, setup.ioNodes.get(0));
            this.setup = setup;
            this.paths = paths;
            this.options = opts;
            this.stats = new ReconstructionStats();
        } catch (IOException | ClaraException e) {
            throw new OrchestratorError("Could not connect to Clara", e);
        }
    }


    public boolean run() {
        try {
            stats.totalTimeStart = System.currentTimeMillis();
            check();
            start();
            stats.recTimeStart = System.currentTimeMillis();
            processFile();
            waitRec();
            stats.recTimeEnd = System.currentTimeMillis();
            stop();
            stats.totalTimeEnd = System.currentTimeMillis();
            end();
            return stats.recStat;
        } catch (OrchestratorError e) {
            end();
            throw e;
        }
    }


    private void check() {
        if (!checkChain()) {
            try {
                deployChain();
            } catch (OrchestratorError e) {
                Logging.info("Cleaning...");
                orchestrator.removeUserContainers();
                throw e;
            }
        }
    }


    private boolean checkChain() {
        for (DpeInfo dpe : setup.ioNodes) {
            Logging.info("Searching I/O services in " + dpe.name);
            if (!orchestrator.findInputOutputService(dpe)) {
                return false;
            }
        }
        for (DpeInfo dpe : setup.recNodes) {
            Logging.info("Deploying reconstruction chain in " + dpe.name);
            if (!orchestrator.findReconstructionServices(dpe)) {
                return false;
            }
        }
        return true;
    }


    private void deployChain() {
        for (DpeInfo dpe : setup.ioNodes) {
            Logging.info("Deploying I/O services in " + dpe.name);
            orchestrator.deployInputOutputServices(dpe, 1);
        }
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (availableProcessors > 12) {
            availableProcessors = 12;
        }
        for (DpeInfo dpe : setup.recNodes) {
            Logging.info("Deploying reconstruction chain in " + dpe.name);
            orchestrator.deployReconstructionChain(dpe, availableProcessors);
        }

        Logging.info("Checking services...");
        for (DpeInfo dpe : setup.ioNodes) {
            orchestrator.checkInputOutputServices(dpe);
        }
        for (DpeInfo dpe : setup.recNodes) {
            orchestrator.checkReconstructionServices(dpe);
        }
    }


    private void start() {
        ioNode.setFiles(paths.inputFile, paths.outputFile);
        ioNode.openFiles(options.reportFreq);

        ErrorHandlerCB errorHandler = new ErrorHandlerCB();
        orchestrator.subscribeErrors(ioNode.containerName, errorHandler);
        orchestrator.subscribeDone(ioNode.writerName, new DataHandlerCB());

        // TODO send proper configuration data
        EngineData configData = new EngineData();
        configData.setData(EngineDataType.STRING.mimeType(), ioNode.currentInputFile);
        for (DpeInfo dpe : setup.recNodes) {
            for (ServiceName recService : reconstructionChains.get(dpe.name)) {
                ioNode.configureService(recService, configData);
            }
        }
    }


    private void processFile() {
        Logging.info("Start processing...");
        try {
            stats.recSem.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("Could not block processing.");
        }

        for (DpeInfo dpe : setup.recNodes) {
            ioNode.sendEventsToDpe(dpe.name, reconstructionChains.get(dpe.name), options.threads);
        }
    }


    private void waitRec() {
        try {
            stats.recSem.acquire();
        } catch (InterruptedException e) {
            stats.recMsg = "Processing interrupted...";
        }
    }


    private void exitRec(boolean status, String msg) {
        stats.recStat = status;
        stats.recMsg = msg;
        stats.recSem.release();
    }


    private void stop() {
        orchestrator.sleep(100);
        ioNode.closeFiles();
    }


    private void end() {
        if (stats.recStat) {
            float recTimeMs = (stats.recTimeEnd - stats.recTimeStart) / 1000.0f;
            float totalTimeMs = (stats.totalTimeEnd - stats.totalTimeStart) / 1000.0f;
            Logging.info("Total processing time   = %.2f s", recTimeMs);
            Logging.info("Total orchestrator time = %.2f s", totalTimeMs);
            Logging.info(stats.recMsg);
        } else {
            Logging.error(stats.recMsg);
        }
    }



    private class DataHandlerCB implements EngineCallback {

        @Override
        public void callback(EngineData data) {
            reportAverage();
        }

        public void reportAverage() {
            long endTime = System.currentTimeMillis();
            ioNode.eventNumber += options.reportFreq;
            double timePerEvent = (endTime - ioNode.startTime) /  (double) ioNode.eventNumber;
            Logging.info("Average event processing time = %.2f ms", timePerEvent);
        }
    }



    private class ErrorHandlerCB implements EngineCallback {

        private boolean stopped = false;

        @Override
        public void callback(EngineData data) {
            handleError(data);
        }

        private synchronized void handleError(EngineData data) {
            if (stopped) {
                return;
            }

            ServiceName source = new ServiceName(data.getEngineName());
            DpeName host = source.dpe();
            int requestId = data.getCommunicationId();
            String description = data.getDescription();

            if (description.equalsIgnoreCase("End of file")) {
                stop(true, "Processing is complete.");
            } else if (description.startsWith("Error opening the file")) {
                Logging.error(description);
                stop(false, "Could not start reconstruction.");
            } else {
                Logging.error("Error in %s (ID: %d):%n%s", source, requestId, description);
                ioNode.requestEvent(host, reconstructionChains.get(host), requestId, "next-rec");
            }
        }

        private void stop(boolean status, String msg) {
            stopped = true;
            exitRec(status, msg);
        }
    }



    private static class CommandLineBuilder {

        private static final String ARG_THREADS = "nThreads";
        private static final String ARG_SERVICES_FILE = "servicesFile";
        private static final String ARG_INPUT_FILE = "inputFile";
        private static final String ARG_OUTPUT_FILE = "outputFile";

        private final JSAP jsap;
        private final JSAPResult config;

        CommandLineBuilder(String[] args) {
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

        public LocalOrchestrator build() {
            final String servicesConfig = config.getString(ARG_SERVICES_FILE);
            final String inFile = config.getString(ARG_INPUT_FILE);
            final String outFile = config.getString(ARG_OUTPUT_FILE);
            final int nc = config.getInt(ARG_THREADS);

            ReconstructionSetup setup = new ReconstructionSetup(servicesConfig);
            ReconstructionPaths paths = new ReconstructionPaths(inFile, outFile);
            ReconstructionOptions opts = new ReconstructionOptions(nc);
            return new LocalOrchestrator(setup, paths, opts);
        }

        private void setArguments(JSAP jsap) {

            FlaggedOption nThreads = new FlaggedOption(ARG_THREADS)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('t')
                    .setDefault("1")
                    .setRequired(false);
            nThreads.setHelp("The number of threads per node for event processing.");

            UnflaggedOption servicesFile = new UnflaggedOption(ARG_SERVICES_FILE)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true);
            servicesFile.setHelp("The YAML file with the reconstruction chain description.");

            UnflaggedOption inputFile = new UnflaggedOption(ARG_INPUT_FILE)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true);
            inputFile.setHelp("The EVIO input file to be reconstructed.");

            UnflaggedOption outputFile = new UnflaggedOption(ARG_OUTPUT_FILE)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true);
            outputFile.setHelp("The EVIO output file where reconstructed events will be saved.");

            try {
                jsap.registerParameter(nThreads);
                jsap.registerParameter(servicesFile);
                jsap.registerParameter(inputFile);
                jsap.registerParameter(outputFile);
            } catch (JSAPException e) {
                throw new RuntimeException(e);
            }
        }
    }



    private static class ConfigFileBuilder {

        private final String configFile;

        ConfigFileBuilder(String configFile) {
            this.configFile = configFile;
        }

        public LocalOrchestrator build() {
            ReconstructionConfigParser parser = new ReconstructionConfigParser(configFile);

            String inFile = parser.parseInputFile();
            String outFile = parser.parseOutputFile();
            int nc = parser.parseNumberOfThreads();

            List<DpeInfo> ioNodes = parser.parseInputOutputNodes();
            List<DpeInfo> recNodes = parser.parseReconstructionNodes();
            List<ServiceInfo> recChain = parser.parseReconstructionChain();

            if (ioNodes.size() > 1) {
                throw new OrchestratorConfigError("only one IO node is supported");
            }

            ReconstructionSetup setup = new ReconstructionSetup(recChain, ioNodes, recNodes);
            ReconstructionPaths paths = new ReconstructionPaths(inFile, outFile);
            ReconstructionOptions opts = new ReconstructionOptions(nc);
            return new LocalOrchestrator(setup, paths, opts);
        }
    }
}
