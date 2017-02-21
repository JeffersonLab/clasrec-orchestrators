package org.jlab.clas.std.orchestrators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.jlab.clara.base.BaseOrchestrator;
import org.jlab.clara.base.ClaraFilters;
import org.jlab.clara.base.ClaraName;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.GenericCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;

class ReconstructionOrchestrator {

    final BaseOrchestrator base;

    private final ServiceInfo stage;
    private final ServiceInfo reader;
    private final ServiceInfo writer;

    private final List<ServiceInfo> reconstructionChain;
    private final Set<ContainerName> userContainers;
    private final Map<ServiceName, DeployedService> userServices;


    ReconstructionOrchestrator(ReconstructionSetup setup, int poolSize) {
        base = new BaseOrchestrator(setup.frontEnd, poolSize);
        reconstructionChain = setReconstructionChain(setup.recChain);

        userContainers = Collections.newSetFromMap(new ConcurrentHashMap<ContainerName, Boolean>());
        userServices = new ConcurrentHashMap<>();

        stage = Objects.requireNonNull(setup.ioServices.get("stage"), "missing stage service");
        reader = Objects.requireNonNull(setup.ioServices.get("reader"), "missing reader service");
        writer = Objects.requireNonNull(setup.ioServices.get("writer"), "missing writer service");

        base.registerDataTypes(EngineDataType.JSON,
                               EngineDataType.STRING,
                               EngineDataType.SFIXED32);
        base.registerDataTypes(setup.dataTypes);
    }


    private static List<ServiceInfo> setReconstructionChain(List<ServiceInfo> reconstructionChain) {
        if (reconstructionChain == null) {
            throw new IllegalArgumentException("null reconstruction chain");
        }
        if (reconstructionChain.isEmpty()) {
            throw new IllegalArgumentException("empty reconstruction chain");
        }
        return new ArrayList<>(reconstructionChain);
    }


    BaseOrchestrator base() {
        return base;
    }


    private void deploy(ServiceInfo service, DpeInfo dpe, int poolsize) {
        try {
            ContainerName containerName = new ContainerName(dpe.name, service.cont);
            if (!userContainers.contains(containerName)) {
                deployContainer(dpe, service.cont);
                userContainers.add(containerName);
            }
            ServiceName serviceName = new ServiceName(containerName, service.name);
            base.deploy(serviceName, service.classpath).withPoolsize(poolsize).run();
            userServices.put(serviceName, new DeployedService(service, dpe, poolsize));
        } catch (ClaraException e) {
            String errorMsg = String.format("failed request to deploy host = '%s' "
                                            + "container = '%s' classpath = '%s'",
                                            dpe.name, service.cont, service.classpath);
            throw new OrchestratorError(errorMsg, e);
        }
    }


    private void deployContainer(DpeInfo dpe, String containerName)
            throws ClaraException {

        ContainerName container = new ContainerName(dpe.name, containerName);
        base.deploy(container).run();

        final int maxAttempts = 10;
        int counter = 0;
        while (true) {
            Set<ContainerName> regContainers = getRegisteredContainers(dpe);
            for (ContainerName c : regContainers) {
                if (container.equals(c)) {
                    return;
                }
            }
            counter++;
            if (counter == 6) {
                base.deploy(container).run();
            }
            if (counter == maxAttempts) {
                throw new OrchestratorError("could not start container = " + container);
            }
            sleep(200);
        }
    }


    void deployInputOutputServices(DpeInfo dpe, int poolsize) {
        deploy(stage, dpe, poolsize);
        deploy(reader, dpe, poolsize);
        deploy(writer, dpe, poolsize);
    }


    void deployReconstructionChain(DpeInfo dpe, int poolsize) {
        for (ServiceInfo service : reconstructionChain) {
            deploy(service, dpe, poolsize);
        }
    }


    ServiceName getStageServiceName(DpeInfo ioDpe) {
        return getServiceName(ioDpe, stage);
    }


    ServiceName getReaderServiceName(DpeInfo ioDpe) {
        return getServiceName(ioDpe, reader);
    }


    ServiceName getWriterServiceName(DpeInfo ioDpe) {
        return getServiceName(ioDpe, writer);
    }


    List<ServiceName> generateReconstructionChain(DpeInfo recDpe) {
        return reconstructionChain.stream()
                .map(service -> getServiceName(recDpe, service))
                .collect(Collectors.toList());
    }


    private Set<ContainerName> getRegisteredContainers(DpeInfo dpe) {
        try {
            return base.query()
                       .canonicalNames(ClaraFilters.containersByDpe(dpe.name))
                       .syncRun(3, TimeUnit.SECONDS);
        } catch (TimeoutException | ClaraException e) {
            throw new OrchestratorError(e);
        }
    }


    private Set<ServiceName> getRegisteredServices(DpeInfo dpe) {
        try {
            return base.query()
                       .canonicalNames(ClaraFilters.servicesByDpe(dpe.name))
                       .syncRun(3, TimeUnit.SECONDS);
        } catch (TimeoutException | ClaraException e) {
            throw new OrchestratorError(e);
        }
    }


    private Set<ServiceName> findMissingServices(Set<ServiceName> services,
                                                 Set<ServiceName> regServices) {
        Set<ServiceName> missingServices = new HashSet<>();
        for (ServiceName s : services) {
            if (!regServices.contains(s)) {
                missingServices.add(s);
            }
        }
        return missingServices;
    }


    private void checkServices(DpeInfo dpe, Set<ServiceName> services) {
        final int sleepTime = 2000;
        final int totalConnectTime = 1000 * 10 * services.size();
        final int maxAttempts = totalConnectTime / sleepTime;
        final int retryAttempts = maxAttempts / 2;

        int counter = 1;
        while (true) {
            Set<ServiceName> regServices = getRegisteredServices(dpe);
            Set<ServiceName> missingServices = findMissingServices(services, regServices);
            if (missingServices.isEmpty()) {
                return;
            } else {
                if (counter == retryAttempts) {
                    reDeploy(regServices, missingServices);
                }
                counter++;
                if (counter > maxAttempts) {
                    throw new OrchestratorError(reportUndeployed(missingServices));
                }
                sleep(2000);
            }
        }
    }


    private void reDeploy(Set<ServiceName> regServices, Set<ServiceName> missingServices) {
        // Remove user containers that were not started
        Set<ContainerName> regContainers = new HashSet<>();
        for (ServiceName service : regServices) {
            regContainers.add(service.container());
        }
        for (ServiceName missing : missingServices) {
            ContainerName cont = missing.container();
            if (!regContainers.contains(cont)) {
                userContainers.remove(cont);
            }
        }
        // Re-deploy missing services
        for (ServiceName missing : missingServices) {
            DeployedService deployInfo = userServices.get(missing);
            Logging.info("Service " + missing + " was not found. Trying to redeploy...");
            deploy(deployInfo.service, deployInfo.dpe, deployInfo.poolsize);
        }
    }


    private String reportUndeployed(Set<ServiceName> missingServices) {
        StringBuilder sb = new StringBuilder();
        sb.append("undeployed service");
        if (missingServices.size() > 1) {
            sb.append("s");
        }
        sb.append(" = '");
        Iterator<ServiceName> iter = missingServices.iterator();
        sb.append(iter.next());
        while (iter.hasNext()) {
            sb.append(", ");
            sb.append(iter.next());
        }
        sb.append("'");
        return sb.toString();
    }


    boolean findInputOutputService(DpeInfo dpe) {
        Set<ServiceName> services = getServiceNames(dpe, Arrays.asList(stage, reader, writer));
        Set<ServiceName> regServices = getRegisteredServices(dpe);
        return findMissingServices(services, regServices).isEmpty();
    }


    boolean findReconstructionServices(DpeInfo dpe) {
        Set<ServiceName> recChain = getServiceNames(dpe, reconstructionChain);
        Set<ServiceName> regServices = getRegisteredServices(dpe);
        return findMissingServices(new HashSet<>(recChain), regServices).isEmpty();
    }


    void checkInputOutputServices(DpeInfo dpe) {
        checkServices(dpe, getServiceNames(dpe, Arrays.asList(stage, reader, writer)));
    }


    void checkReconstructionServices(DpeInfo dpe) {
        checkServices(dpe, getServiceNames(dpe, reconstructionChain));
    }


    private ServiceName getServiceName(DpeInfo dpe, ServiceInfo service) {
        return new ServiceName(dpe.name, service.cont, service.name);
    }


    private Set<ServiceName> getServiceNames(DpeInfo dpe, Collection<ServiceInfo> services) {
        return services.stream()
                .map(service -> getServiceName(dpe, service))
                .collect(Collectors.toSet());
    }


    void subscribeDpes(DpeCallBack callback, String session) {
        try {
            DpeCallbackWrapper dpeCallback = new DpeCallbackWrapper(callback);
            base.listen().aliveDpes(session).start(dpeCallback);
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not subscribe to front-end to get running DPEs", e);
        }
    }


    void subscribeErrors(ClaraName name, EngineCallback callback) {
        try {
            base.listen(name).status(EngineStatus.ERROR).start(callback);
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not subscribe to services", e);
        }
    }


    void subscribeDone(ServiceName service, EngineCallback callback) {
        try {
            base.listen(service).done().start(callback);
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not subscribe to services", e);
        }
    }


    Set<ServiceRuntimeData> getReport(DpeName dpe) {
        try {
            return base.query()
                       .runtimeData(ClaraFilters.servicesByDpe(dpe))
                       .syncRun(5, TimeUnit.SECONDS);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError(e);
        }
    }


    DpeName getFrontEnd() {
        return base.getFrontEnd();
    }


    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    private class DeployedService {

        final ServiceInfo service;
        final DpeInfo dpe;
        final int poolsize;

        DeployedService(ServiceInfo service, DpeInfo dpe, int poolsize) {
            this.service = service;
            this.dpe = dpe;
            this.poolsize = poolsize;
        }
    }



    interface DpeCallBack {
        void callback(DpeInfo dpe);
    }



    private class DpeCallbackWrapper implements GenericCallback {

        final DpeCallBack callback;

        DpeCallbackWrapper(DpeCallBack callback) {
            this.callback = callback;
        }

        @Override
        public void callback(String data) {
            try {
                StringTokenizer st = new StringTokenizer(data, "?");
                DpeName name = new DpeName(st.nextToken());
                int ncores = Integer.parseInt(st.nextToken());
                String claraHome = st.nextToken();
                DpeInfo dpe = new DpeInfo(name, ncores, claraHome);
                callback.callback(dpe);
            } catch (NoSuchElementException e) {
                e.printStackTrace();
            }
        }
    }
}
