package org.jlab.clas.std.orchestrators;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jlab.clara.base.Composition;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;
import org.json.JSONObject;


class ReconstructionNode {

    private final ReconstructionOrchestrator orchestrator;

    final DpeInfo dpe;
    final ContainerName containerName;
    final ServiceName stageName;
    final ServiceName readerName;
    final ServiceName writerName;

    String currentInputFileName;
    String currentInputFile;
    String currentOutputFile;

    int totalEvents;
    int eventNumber;
    long startTime;


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


    void setPaths(String inputPath, String outputPath, String stagePath) {
        JSONObject data = new JSONObject();
        data.put("input_path", inputPath);
        data.put("output_path", outputPath);
        data.put("stage_path", stagePath);
        syncConfig(stageName, data, 2, TimeUnit.MINUTES);
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

            Logging.info("Staging file %s on %s", currentInputFileName, dpe.name);
            EngineData result = syncSend(stageName, data, 5, TimeUnit.MINUTES);

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


    boolean saveOutputFile() {
        try {
            JSONObject cleanRequest = new JSONObject();
            cleanRequest.put("type", "exec");
            cleanRequest.put("action", "remove_input");
            cleanRequest.put("file", currentInputFileName);
            EngineData rr = syncSend(stageName, cleanRequest, 5, TimeUnit.MINUTES);

            JSONObject saveRequest = new JSONObject();
            saveRequest.put("type", "exec");
            saveRequest.put("action", "save_output");
            saveRequest.put("file", currentInputFileName);
            EngineData rs = syncSend(stageName, saveRequest, 5, TimeUnit.MINUTES);

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


    void openFiles() {
        startTime = 0;
        eventNumber = 0;
        totalEvents = 0;

        // open input file
        Logging.info("Opening file %s on %s", currentInputFileName, dpe.name);
        JSONObject inputConfig = new JSONObject();
        inputConfig.put("action", "open");
        inputConfig.put("file", currentInputFile);
        syncConfig(readerName, inputConfig, 5, TimeUnit.MINUTES);

        // total number of events in the file
        totalEvents = requestNumberOfEvents();

        // endiannes of the file
        String fileOrder = requestFileOrder();

        // open output file
        JSONObject outputConfig = new JSONObject();
        outputConfig.put("action", "open");
        outputConfig.put("file", currentOutputFile);
        outputConfig.put("order", fileOrder);
        outputConfig.put("overwrite", true);
        syncConfig(writerName, outputConfig, 5, TimeUnit.MINUTES);
    }


    void setReportFrequency(int frequency) {
        // set "report done" frequency
        if (frequency <= 0) {
            return;
        }
        try {
            orchestrator.base.configure(writerName).startDoneReporting(frequency).run();
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not configure service = " + writerName, e);
        }
    }


    void closeFiles() {
        JSONObject closeInput = new JSONObject();
        closeInput.put("action", "close");
        closeInput.put("file", currentInputFile);
        syncConfig(readerName, closeInput, 5, TimeUnit.MINUTES);

        JSONObject closeOutput = new JSONObject();
        closeOutput.put("action", "close");
        closeOutput.put("file", currentOutputFile);
        syncConfig(writerName, closeOutput, 5, TimeUnit.MINUTES);
    }


    private String requestFileOrder() {
        try {
            EngineData output = syncSend(readerName, "order", 1, TimeUnit.MINUTES);
            return (String) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not get input file order", e);
        }
    }


    private int requestNumberOfEvents() {
        try {
            EngineData output = syncSend(readerName, "count", 1, TimeUnit.MINUTES);
            return (Integer) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not get number of input events", e);
        }
    }


    void configureService(ServiceName service, EngineData data) {
        try {
            orchestrator.base.configure(service)
                             .withData(data)
                             .syncRun(2, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not configure " + service, e);
        }
    }


    void sendEventsToDpe(DpeName dpeName, List<ServiceName> chain, int dpeCores) {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        Logging.info("Using %d cores on %s to reconstruct %d events of %s",
                      dpeCores, dpeName, totalEvents, currentInputFileName);

        int requestId = 1;
        for (int i = 0; i < dpeCores; i++) {
            requestEvent(dpeName, chain, requestId++, "next");
        }
    }


    void requestEvent(DpeName dpeName, List<ServiceName> chain, int requestId, String type) {
        try {
            EngineData data = new EngineData();
            data.setData(EngineDataType.STRING.mimeType(), type);
            data.setCommunicationId(requestId);
            Composition composition = generateComposition(chain);
            orchestrator.base.execute(composition).withData(data).run();
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not request reconstruction on = " + dpeName, e);
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


    private void syncConfig(ServiceName service, JSONObject data, int wait, TimeUnit unit) {
        try {
            EngineData input = new EngineData();
            input.setData(EngineDataType.JSON.mimeType(), data.toString());
            orchestrator.base.configure(service)
                             .withData(input)
                             .syncRun(wait, unit);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not configure service = " + service, e);
        }
    }


    private EngineData syncSend(ServiceName service, String data, int wait, TimeUnit unit)
            throws ClaraException, TimeoutException {
        EngineData input = new EngineData();
        input.setData(EngineDataType.STRING.mimeType(), data);
        return syncSend(service, input, wait, unit);
    }


    private EngineData syncSend(ServiceName service, JSONObject data, int wait, TimeUnit unit)
            throws ClaraException, TimeoutException {
        EngineData input = new EngineData();
        input.setData(EngineDataType.JSON.mimeType(), data.toString());
        return syncSend(service, input, wait, unit);
    }


    private EngineData syncSend(ServiceName service, EngineData input, int wait, TimeUnit unit)
            throws ClaraException, TimeoutException {
        EngineData output = orchestrator.base.execute(service)
                                             .withData(input)
                                             .syncRun(wait, unit);
        if (output.getStatus() == EngineStatus.ERROR) {
            throw new ClaraException(output.getDescription());
        }
        return output;
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
}
