package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Benchmark {

    final Map<ServiceInfo, Runtime> runtimeStats = new HashMap<>();

    Benchmark(List<ServiceInfo> services) {
        services.forEach(s -> {
            runtimeStats.put(s, new Runtime());
        });
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
