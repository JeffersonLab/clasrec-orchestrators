package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.Composition;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.ServiceName;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ReconstructionApplication {

    private final ApplicationInfo application;
    private final DpeInfo dpe;


    ReconstructionApplication(ApplicationInfo application, DpeInfo dpe) {
        this.application = application;
        this.dpe = dpe;
    }


    public ServiceName stageService() {
        return toName(application.getStageService());
    }


    public ServiceName readerService() {
        return toName(application.getReaderService());
    }


    public ServiceName writerService() {
        return toName(application.getWriterService());
    }


    public List<ServiceName> recServices() {
        return application.getRecServices().stream()
                .map(this::toName)
                .collect(Collectors.toList());
    }


    public Composition composition() {
        String composition = readerService().canonicalName();
        for (ServiceName service : recServices()) {
            composition += "+" + service.canonicalName();
        }
        composition += "+" + writerService().canonicalName();
        composition += "+" + readerService().canonicalName();
        composition += ";";
        return new Composition(composition);
    }


    private ServiceName toName(ServiceInfo service) {
        return new ServiceName(dpe.name, service.cont, service.name);
    }


    Stream<DeployInfo> getIODeployInfo() {
        return application.getIOServices().stream()
                          .map(s -> new DeployInfo(toName(s), s.classpath, 1));
    }


    Stream<DeployInfo> getRecDeployInfo() {
        int recCores = maxCores();
        return application.getRecServices().stream()
                          .distinct()
                          .map(s -> new DeployInfo(toName(s), s.classpath, recCores));
    }


    Set<ServiceName> allServices() {
        return application.getAllServices().stream()
                .map(this::toName)
                .collect(Collectors.toSet());
    }


    Set<ContainerName> allContainers() {
        return allServices().stream()
                .map(ServiceName::container)
                .collect(Collectors.toSet());
    }


    DpeName dpe() {
        return dpe.name;
    }


    public int maxCores() {
        return dpe.cores;
    }


    public String hostName() {
        return dpe.name.address().host();
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        ReconstructionApplication other = (ReconstructionApplication) obj;
        if (!dpe.equals(other.dpe)) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return "[" + dpe.name + "]";
    }
}
