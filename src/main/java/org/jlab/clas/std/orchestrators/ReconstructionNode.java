package org.jlab.clas.std.orchestrators;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.jlab.clara.base.Composition;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;
import org.json.JSONObject;


class ReconstructionNode {

    private final ReconstructionOrchestrator orchestrator;

    private final DpeInfo dpe;

    private final ContainerName containerName;
    private final ServiceName stageName;
    private final ServiceName readerName;
    private final ServiceName writerName;

    volatile ReconstructionFile recFile;

    volatile String currentInputFileName;
    volatile String currentInputFile;
    volatile String currentOutputFile;

    AtomicInteger currentFileCounter = new AtomicInteger();
    AtomicInteger totalFilesCounter = new AtomicInteger();

    AtomicInteger totalEvents = new AtomicInteger();
    AtomicInteger eventNumber = new AtomicInteger();
    AtomicLong startTime = new AtomicLong();


    ReconstructionNode(ReconstructionOrchestrator orchestrator, DpeInfo dpe) {
        if (orchestrator == null) {
            throw new IllegalArgumentException("Null orchestrator parameter");
        }
        if (dpe == null) {
            throw new IllegalArgumentException("Null DPE parameter");
        }

        this.dpe = dpe;
        this.orchestrator = orchestrator;

        this.containerName = new ContainerName(dpe.name,
                                               ReconstructionConfigParser.getDefaultContainer());
        this.stageName = orchestrator.getStageServiceName(dpe);
        this.readerName = orchestrator.getReaderServiceName(dpe);
        this.writerName = orchestrator.getWriterServiceName(dpe);
    }


    void deployServices() {
        orchestrator.deployInputOutputServices(dpe, 1);
        orchestrator.deployReconstructionChain(dpe, dpe.cores);
        orchestrator.checkInputOutputServices(dpe);
        orchestrator.checkReconstructionServices(dpe);
    }


    boolean checkServices() {
        if (!orchestrator.findInputOutputService(dpe)) {
            return false;
        }
        if (!orchestrator.findReconstructionServices(dpe)) {
            return false;
        }
        return true;
    }


    void subscribeErrors(Function<ReconstructionNode, EngineCallback> callbackFn) {
        orchestrator.subscribeErrors(containerName, callbackFn.apply(this));
    }


    void subscribeDone(Function<ReconstructionNode, EngineCallback> callbackFn) {
        orchestrator.subscribeDone(writerName, callbackFn.apply(this));
    }


    void setPaths(String inputPath, String outputPath, String stagePath) {
        try {
            JSONObject data = new JSONObject();
            data.put("input_path", inputPath);
            data.put("output_path", outputPath);
            data.put("stage_path", stagePath);
            orchestrator.syncConfig(stageName, data, 2, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not configure directories", e);
        }
    }


    boolean setFiles(String inputFileName) {
        try {
            currentInputFileName = inputFileName;
            currentInputFile = ClaraConstants.UNDEFINED;
            currentOutputFile = ClaraConstants.UNDEFINED;

            JSONObject data = new JSONObject();
            data.put("type", "exec");
            data.put("action", "stage_input");
            data.put("file", currentInputFileName);

            Logging.info("Staging file %s on %s", currentInputFileName, name());
            EngineData result = orchestrator.syncSend(stageName, data, 5, TimeUnit.MINUTES);

            if (!result.getStatus().equals(EngineStatus.ERROR)) {
                String rs = (String) result.getData();
                JSONObject rd = new JSONObject(rs);
                currentInputFile = rd.getString("input_file");
                currentOutputFile = rd.getString("output_file");
                return true;
            } else {
                System.err.println(result.getDescription());
                currentInputFileName = ClaraConstants.UNDEFINED;
                return false;
            }
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not configure directories", e);
        }
    }


    void setFiles(String inputFile, String outputFile) {
        currentInputFile = inputFile;
        currentOutputFile = outputFile;
        currentInputFileName = new File(inputFile).getName();
    }


    void setFileCounter(int currentFile, int totalFiles) {
        currentFileCounter.set(currentFile);
        totalFilesCounter.set(totalFiles);
    }


    boolean saveOutputFile() {
        try {
            JSONObject cleanRequest = new JSONObject();
            cleanRequest.put("type", "exec");
            cleanRequest.put("action", "remove_input");
            cleanRequest.put("file", currentInputFileName);
            EngineData rr = orchestrator.syncSend(stageName, cleanRequest, 5, TimeUnit.MINUTES);

            JSONObject saveRequest = new JSONObject();
            saveRequest.put("type", "exec");
            saveRequest.put("action", "save_output");
            saveRequest.put("file", currentInputFileName);
            EngineData rs = orchestrator.syncSend(stageName, saveRequest, 5, TimeUnit.MINUTES);

            currentInputFileName = ClaraConstants.UNDEFINED;
            currentInputFile = ClaraConstants.UNDEFINED;
            currentOutputFile = ClaraConstants.UNDEFINED;

            boolean status = true;
            if (rr.getStatus().equals(EngineStatus.ERROR)) {
                System.err.println(rr.getDescription());
                status = false;
            }
            if (rs.getStatus().equals(EngineStatus.ERROR)) {
                status = false;
                System.err.println(rs.getDescription());
            }

            return status;
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not save output", e);
        }
    }


    boolean removeStageDir() {
        try {
            JSONObject request = new JSONObject();
            request.put("type", "exec");
            request.put("action", "clear_stage");
            request.put("file", currentInputFileName);
            EngineData rr = orchestrator.syncSend(stageName, request, 5, TimeUnit.MINUTES);
            if (rr.getStatus().equals(EngineStatus.ERROR)) {
                System.err.println(rr.getDescription());
                return false;
            }
            return true;
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not save output", e);
        }
    }


    void openFiles() {
        startTime.set(0);
        eventNumber.set(0);
        totalEvents.set(0);

        // open input file
        try {
            Logging.info("Opening file %s on %s", currentInputFileName, name());
            JSONObject inputConfig = new JSONObject();
            inputConfig.put("action", "open");
            inputConfig.put("file", currentInputFile);
            orchestrator.syncConfig(readerName, inputConfig, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not open input file", e);
        }

        // total number of events in the file
        totalEvents.set(requestNumberOfEvents());

        // endiannes of the file
        String fileOrder = requestFileOrder();

        // open output file
        try {
            JSONObject outputConfig = new JSONObject();
            outputConfig.put("action", "open");
            outputConfig.put("file", currentOutputFile);
            outputConfig.put("order", fileOrder);
            outputConfig.put("overwrite", true);
            orchestrator.syncConfig(writerName, outputConfig, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not open output file", e);
        }
    }


    void setReportFrequency(int frequency) {
        // set "report done" frequency
        if (frequency <= 0) {
            return;
        }
        try {
            orchestrator.startDoneReporting(writerName, frequency);
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not configure writer", e);
        }
    }


    void closeFiles() {
        try {
            JSONObject closeInput = new JSONObject();
            closeInput.put("action", "close");
            closeInput.put("file", currentInputFile);
            orchestrator.syncConfig(readerName, closeInput, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not close input file", e);
        }

        try {
            JSONObject closeOutput = new JSONObject();
            closeOutput.put("action", "close");
            closeOutput.put("file", currentOutputFile);
            orchestrator.syncConfig(writerName, closeOutput, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not close output file", e);
        }
    }


    private String requestFileOrder() {
        try {
            EngineData output = orchestrator.syncSend(readerName, "order", 1, TimeUnit.MINUTES);
            return (String) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not get input file order", e);
        }
    }


    private int requestNumberOfEvents() {
        try {
            EngineData output = orchestrator.syncSend(readerName, "count", 1, TimeUnit.MINUTES);
            return (Integer) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not get number of input events", e);
        }
    }


    void configureServices(EngineData data) {
        for (ServiceName service : orchestrator.generateReconstructionChain(dpe)) {
            try {
                orchestrator.syncConfig(service, data, 2, TimeUnit.MINUTES);
            } catch (ClaraException | TimeoutException e) {
                throw new OrchestratorError("Could not configure " + service, e);
            }
        }
    }


    void sendEvents(int maxCores) {
        startTime.compareAndSet(0, System.currentTimeMillis());

        int requestCores = numCores(maxCores);
        int requestId = 1;

        Logging.info("Using %d cores on %s to reconstruct %d events of %s [%d/%d]",
                      requestCores, name(), totalEvents.get(), currentInputFileName,
                      currentFileCounter.get(), totalFilesCounter.get());

        for (int i = 0; i < requestCores; i++) {
            requestEvent(requestId++, "next");
        }
    }


    void requestEvent(int requestId, String type) {
        try {
            EngineData data = new EngineData();
            data.setData(EngineDataType.STRING.mimeType(), type);
            data.setCommunicationId(requestId);
            List<ServiceName> chain = orchestrator.generateReconstructionChain(dpe);
            orchestrator.send(generateComposition(chain), data);
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not request reconstruction on = " + name(), e);
        }
    }


    private Composition generateComposition(List<ServiceName> chain) {
        String composition = readerName.canonicalName();
        for (ServiceName service : chain) {
            composition += "+" + service.canonicalName();
        }
        composition += "+" + writerName.canonicalName();
        composition += "+" + readerName.canonicalName();
        composition += ";";
        return new Composition(composition);
    }


    private int numCores(int maxCores) {
        return dpe.cores <= maxCores ? dpe.cores : maxCores;
    }


    Set<ServiceRuntimeData> getRuntimeData() {
        return orchestrator.getReport(dpe.name);
    }


    boolean isFrontEnd() {
        return dpe.name.equals(orchestrator.getFrontEnd());
    }


    String name() {
        return dpe.name.canonicalName();
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + dpe.hashCode();
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ReconstructionNode)) {
            return false;
        }
        ReconstructionNode other = (ReconstructionNode) obj;
        if (!dpe.equals(other.dpe)) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return dpe.name.toString();
    }
}
