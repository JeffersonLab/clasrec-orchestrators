package org.jlab.clas.std.orchestrators;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas.std.orchestrators.ReconstructionConfigParser.ConfigFileChecker;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;
import org.json.JSONObject;
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
    private final Benchmark benchmark;

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
                    System.err.printf("Usage:%n%n  local-orchestrator %s%n%n%n%s",
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

        private final Map<String, ServiceInfo> ioServices;
        private final List<ServiceInfo> recChain;
        private DpeName frontEnd;

        private final Set<EngineDataType> dataTypes;
        private final JSONObject config;
        private final String session;

        private final String inputFile;
        private String outputFile;

        private final ReconstructionOptions.Builder options;


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

            this.ioServices = parser.parseInputOutputServices();
            this.recChain = parser.parseReconstructionChain();
            this.frontEnd = ReconstructionConfigParser.localDpeName();

            this.config = parser.parseReconstructionConfig();
            this.dataTypes = parser.parseDataTypes();
            this.session = "";

            this.options = ReconstructionOptions.builder()
                    .withPoolSize(2)
                    .withMaxThreads(1)
                    .withReportFrequency(500);

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
            options.withMaxThreads(numThreads);
            return this;
        }

        /**
         * Sets the frequency of the "done" reports by the standard writer.
         */
        public Builder withReportFrequency(int frequency) {
            options.withReportFrequency(frequency);
            return this;
        }

        /**
         * Sets the number of events to skip.
         */
        public Builder withSkipEvents(int skip) {
            options.withSkipEvents(skip);
            return this;
        }

        /**
         * Sets the maximum number of events to read.
         */
        public Builder withMaxEvents(int max) {
            options.withMaxEvents(max);
            return this;
        }

        /**
         * Sets the name of the front-end DPE. Use this if the orchestrator is not
         * running in the same node as the front-end, or if the orchestrator is
         * not using the proper network interface or port for the DPE.
         */
        public Builder withFrontEnd(DpeName frontEnd) {
            Objects.requireNonNull(frontEnd, "DPE parameter is null");
            this.frontEnd = frontEnd;
            return this;
        }

        /**
         * Creates the orchestrator.
         */
        public LocalOrchestrator build() {
            ReconstructionSetup setup = new ReconstructionSetup(frontEnd, ioServices, recChain,
                                                                dataTypes, config, session);
            ReconstructionPaths paths = new ReconstructionPaths(inputFile, outputFile);

            return new LocalOrchestrator(setup, paths, options.build());
        }
    }


    private LocalOrchestrator(ReconstructionSetup setup,
                              ReconstructionPaths paths,
                              ReconstructionOptions opts) {
        super(setup, paths, opts);
        ioNode = localNode();
        benchmark = new Benchmark(setup.application);
    }


    @Override
    void start() {
        orchTimeStart = System.currentTimeMillis();
        printStartup();
        setupNode(ioNode);
        benchmark.initialize(ioNode.getRuntimeData());
    }


    private void printStartup() {
        System.out.println("****************************************");
        System.out.println("*        CLAS Local Orchestrator       *");
        System.out.println("****************************************");
        if (setup.application.getLanguages().size() == 1) {
            System.out.println("- Local DPE    = " + setup.frontEnd);
        } else {
            List<DpeName> dpes = multiLangDpes();
            System.out.println("- Local DPEs   = " + dpes.get(0));
            for (int i = 1; i < dpes.size(); i++) {
                System.out.println("                 " + dpes.get(i));
            }
            System.out.println();
        }
        System.out.println("- Start time   = " + ClaraUtil.getCurrentTime());
        System.out.println("- Threads      = " + options.maxThreads);
        System.out.println();
        System.out.println("- Input file   = " + paths.inputFilePath(paths.allFiles.get(0)));
        System.out.println("- Output file  = " + paths.outputFilePath(paths.allFiles.get(0)));
        System.out.println("****************************************");
    }


    private List<DpeName> multiLangDpes() {
        return ioNode.dpes().stream()
            .collect(Collectors.groupingBy(DpeName::language, TreeMap::new, Collectors.toList()))
            .values().stream()
            .map(list -> list.get(0))
            .collect(Collectors.toList());
    }


    @Override
    void subscribe(ReconstructionNode node) {
        super.subscribe(node);
        node.subscribeDone(n -> new DataHandlerCB());
    }


    @Override
    void end() {
        try {
            benchmark.update(ioNode.getRuntimeData());
            BenchmarkPrinter printer = new BenchmarkPrinter(benchmark, stats.totalEvents());
            printer.printBenchmark(setup.application);
        } catch (OrchestratorError e) {
            Logging.error("Could not generate benchmark: %s", e.getMessage());
        }

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
            Logging.info("  Processed  %5d events  " +
                         "  total time = %7.2f s  " +
                         "  average event time = %6.2f ms",
                         totalEvents, totalTime / 1000L, timePerEvent);
        }
    }


    private static class CommandLineBuilder {

        private static final String ARG_FRONTEND = "frontEnd";
        private static final String ARG_THREADS = "nThreads";
        private static final String ARG_FREQUENCY = "frequency";
        private static final String ARG_SKIP_EVENTS = "skip";
        private static final String ARG_MAX_EVENTS = "max";
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
            final int reportFreq = config.getInt(ARG_FREQUENCY);
            final int nc = config.getInt(ARG_THREADS);
            final DpeName frontEnd = parseFrontEnd();

            List<DpeInfo> ioNodes = new ArrayList<DpeInfo>();
            ioNodes.add(ReconstructionConfigParser.getDefaultDpeInfo("localhost"));

            List<DpeInfo> recNodes = new ArrayList<DpeInfo>();
            recNodes.add(ReconstructionConfigParser.getDefaultDpeInfo("localhost"));

            Builder builder = new Builder(servicesConfig, inFile)
                    .withOutputFile(outFile)
                    .withFrontEnd(frontEnd)
                    .withThreads(nc)
                    .withReportFrequency(reportFreq);

            if (config.contains(ARG_SKIP_EVENTS)) {
                builder.withSkipEvents(config.getInt(ARG_SKIP_EVENTS));
            }
            if (config.contains(ARG_MAX_EVENTS)) {
                builder.withMaxEvents(config.getInt(ARG_MAX_EVENTS));
            }

            return builder.build();
        }

        private DpeName parseFrontEnd() {
            String frontEnd = config.getString(ARG_FRONTEND)
                                    .replaceFirst("localhost", ClaraUtil.localhost());
            try {
                return new DpeName(frontEnd);
            } catch (IllegalArgumentException e) {
                throw new OrchestratorConfigError("invalid DPE name: " + frontEnd);
            }
        }

        private void setArguments(JSAP jsap) {

            FlaggedOption frontEnd = new FlaggedOption(ARG_FRONTEND)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setShortFlag('f')
                    .setDefault(ReconstructionConfigParser.localDpeName().toString())
                    .setRequired(false);
            frontEnd.setHelp("The name of the CLARA front-end DPE.");

            FlaggedOption nThreads = new FlaggedOption(ARG_THREADS)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('t')
                    .setDefault("1")
                    .setRequired(false);
            nThreads.setHelp("The number of threads for event processing.");

            FlaggedOption reportFreq = new FlaggedOption(ARG_FREQUENCY)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('r')
                    .setDefault("500")
                    .setRequired(false);
            reportFreq.setHelp("The report frequency of processed events.");

            FlaggedOption skipEvents = new FlaggedOption(ARG_SKIP_EVENTS)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('k')
                    .setRequired(false);
            skipEvents.setHelp("The amount of events to skip at the beginning.");

            FlaggedOption maxEvents = new FlaggedOption(ARG_MAX_EVENTS)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('e')
                    .setRequired(false);
            maxEvents.setHelp("The maximum number of events to process.");

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
                jsap.registerParameter(frontEnd);
                jsap.registerParameter(nThreads);
                jsap.registerParameter(reportFreq);
                jsap.registerParameter(skipEvents);
                jsap.registerParameter(maxEvents);
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

            ReconstructionSetup setup = new ReconstructionSetup(
                    ReconstructionConfigParser.localDpeName(),
                    parser.parseInputOutputServices(),
                    parser.parseReconstructionChain(),
                    parser.parseDataTypes(),
                    parser.parseReconstructionConfig(),
                    "");

            String inFile = parser.parseInputFile();
            String outFile = parser.parseOutputFile();
            int nc = parser.parseNumberOfThreads();

            ReconstructionPaths paths = new ReconstructionPaths(inFile, outFile);
            ReconstructionOptions opts = ReconstructionOptions.builder()
                    .withPoolSize(2)
                    .withMaxThreads(nc)
                    .withReportFrequency(1000)
                    .build();
            return new LocalOrchestrator(setup, paths, opts);
        }
    }
}
