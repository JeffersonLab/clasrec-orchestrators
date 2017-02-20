package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Benchmark {

    final Map<ServiceInfo, Runtime> runtimeStats = new HashMap<>();

    Benchmark(ReconstructionSetup setup) {
        List<ServiceInfo> services = allServices(setup);
        services.forEach(s -> {
            runtimeStats.put(s, new Runtime());
        });
    }

    private List<ServiceInfo> allServices(ReconstructionSetup setup) {
        List<ServiceInfo> services = new ArrayList<>();
        services.add(setup.ioServices.get("reader"));
        services.addAll(setup.recChain);
        services.add(setup.ioServices.get("writer"));
        return services;
    }

    void initialize(Set<ServiceRuntimeData> data) {
        data.forEach(s -> {
            Runtime r = runtimeStats.get(key(s.name()));
            if (r != null) {
                r.initialTime = s.executionTime();
            }
        });
    }

    void update(Set<ServiceRuntimeData> data) {
        data.forEach(s -> {
            Runtime r = runtimeStats.get(key(s.name()));
            if (r != null) {
                r.totalTime = s.executionTime();
            }
        });
    }

    long time(ServiceInfo service) {
        Runtime r = runtimeStats.get(service);
        if (r != null) {
            return r.totalTime - r.initialTime;
        }
        throw new OrchestratorError("Invalid runtime report: missing " + service.name);
    }

    private static ServiceInfo key(ServiceName service) {
        return new ServiceInfo("", service.container().name(), service.name());
    }

    private static class Runtime {
        long initialTime = 0;
        long totalTime = 0;
    }
}
