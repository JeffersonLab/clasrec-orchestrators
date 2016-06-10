package org.jlab.clas.std.orchestrators;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.engine.EngineData;
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
public final class LocalOrchestrator extends AbstractOrchestrator {

    private final ReconstructionNode ioNode;

    private long orchTimeStart;
    private long orchTimeEnd;

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


    /**
     * Helps constructing a {@link LocalOrchestrator} with all default and
     * required parameters.
     */
    public static final class Builder {

        private final List<ServiceInfo> recChain;
        private final String localhost;

        private String inputFile;
        private String outputFile;

        private int threads = 1;
        private int reportFreq = 1000;

        /**
         * Sets the required arguments to start a reconstruction.
         *
         * @param servicesFile the YAML file describing the reconstruction chain
         * @param inputFile the file to be processed
         * @throws OrchestratorConfigError if the reconstruction chain could not be parsed
         */
        public Builder(String servicesFile, String inputFile) {
            Objects.requireNonNull(servicesFile, "servicesFile parameter is null");
            Objects.requireNonNull(inputFile, "inputFile parameter is null");
            if (inputFile.isEmpty()) {
                throw new IllegalArgumentException("inputFile parameter is empty");
            }

            ReconstructionConfigParser parser = new ReconstructionConfigParser(servicesFile);

            this.recChain = parser.parseReconstructionChain();
            this.localhost = ReconstructionConfigParser.hostAddress("localhost");
            this.inputFile = inputFile;

            Path inputFilePath = Paths.get(inputFile);
            Path inputDirectory = inputFilePath.getParent();
            String outputFileName = "out_" + inputFilePath.getFileName();
            if (inputDirectory != null) {
                this.outputFile = Paths.get(inputDirectory.toString(), outputFileName).toString();
            } else {
                this.outputFile = Paths.get(outputFileName).toString();
            }
        }

        /**
         * Sets the path to the reconstructed output file.
         */
        public Builder withOutputFile(String outputFile) {
            Objects.requireNonNull(outputFile, "outputFile parameter is null");
            if (outputFile.isEmpty()) {
                throw new IllegalArgumentException("outputFile parameter is empty");
            }
            this.outputFile = outputFile;
            return this;
        }

        /**
         * Sets the number of threads to be used for reconstruction.
         */
        public Builder withThreads(int numThreads) {
            if (numThreads <= 0) {
                throw new IllegalArgumentException("Invalid number of threads: " + numThreads);
            }
            this.threads = numThreads;
            return this;
        }

        /**
         * Sets the frequency of the "done" reports by the standard writer.
         */
        public Builder withReportFrequency(int frequency) {
            if (frequency <= 0) {
                throw new IllegalArgumentException("Invalid number of threads: " + frequency);
            }
            this.reportFreq = frequency;
            return this;
        }

        /**
         * Creates the orchestrator.
         */
        public LocalOrchestrator build() {
            ReconstructionSetup setup = new ReconstructionSetup(recChain, localhost);
            ReconstructionPaths paths = new ReconstructionPaths(inputFile, outputFile);
            ReconstructionOptions opts = new ReconstructionOptions(false, 2, threads, reportFreq);
            return new LocalOrchestrator(setup, paths, opts);
        }
    }


    private LocalOrchestrator(ReconstructionSetup setup,
                              ReconstructionPaths paths,
                              ReconstructionOptions opts) {
        super(setup, paths, opts);
        DpeName name = new DpeName(setup.localHost, ClaraLang.JAVA);
        int cores = Runtime.getRuntime().availableProcessors();
        DpeInfo dpe = new DpeInfo(name, cores, DpeInfo.DEFAULT_CLARA_HOME);
        ioNode = new ReconstructionNode(orchestrator, dpe);
    }


    @Override
    void start() {
        orchTimeStart = System.currentTimeMillis();
        setupNode(ioNode);
    }


    @Override
    void subscribe(ReconstructionNode node) {
        super.subscribe(node);
        orchestrator.subscribeDone(node.writerName, new DataHandlerCB());
    }


    @Override
    void end() {
        orchTimeEnd = System.currentTimeMillis();
        float recTimeMs = stats.totalTime() / 1000.0f;
        float totalTimeMs = (orchTimeEnd - orchTimeStart) / 1000.0f;
        System.out.println();
        Logging.info("Average processing time  = %7.2f ms", stats.localAverage());
        Logging.info("Total processing time    = %7.2f s", recTimeMs);
        Logging.info("Total orchestrator time  = %7.2f s", totalTimeMs);
    }


    private class DataHandlerCB implements EngineCallback {

        @Override
        public void callback(EngineData data) {
            int totalEvents = ioNode.eventNumber.addAndGet(options.reportFreq);
            long endTime = System.currentTimeMillis();
            double totalTime = (endTime - ioNode.startTime.get());
            double timePerEvent = totalTime /  totalEvents;
            Logging.info("Average event processing time = %.2f ms", timePerEvent);
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

            List<DpeInfo> ioNodes = new ArrayList<DpeInfo>();
            ioNodes.add(ReconstructionConfigParser.getDefaultDpeInfo("localhost"));

            List<DpeInfo> recNodes = new ArrayList<DpeInfo>();
            recNodes.add(ReconstructionConfigParser.getDefaultDpeInfo("localhost"));

            Builder builder = new Builder(servicesConfig, inFile);
            return builder.withOutputFile(outFile).withThreads(nc).build();
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

            List<ServiceInfo> recChain = parser.parseReconstructionChain();

            ReconstructionSetup setup = new ReconstructionSetup(recChain, "localhost");
            ReconstructionPaths paths = new ReconstructionPaths(inFile, outFile);
            ReconstructionOptions opts = new ReconstructionOptions(false, 2, nc, 1000);
            return new LocalOrchestrator(setup, paths, opts);
        }
    }
}
