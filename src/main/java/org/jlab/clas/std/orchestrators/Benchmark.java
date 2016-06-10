package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.ServiceName;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Benchmark {

    Map<String, Runtime> runtimeStats = new HashMap<>();

    Benchmark(List<ServiceName> services) {
        services.forEach(s -> {
            runtimeStats.put(s.canonicalName(), new Runtime());
        });
    }

    void initialize(String data) {
        try {
            Map<String, JSONObject> runtime = parseData(new JSONObject(data));
            runtime.forEach((n, d) -> {
                Runtime r = runtimeStats.get(n);
                if (r != null) {
                    r.initialTime = d.getLong("exec_time");
                }
            });
        } catch (JSONException e) {
            throw new OrchestratorError("Invalid runtime report: " + e.getMessage());
        }
    }

    void update(String data) {
        try {
            Map<String, JSONObject> runtime = parseData(new JSONObject(data));
            runtime.forEach((n, d) -> {
                Runtime r = runtimeStats.get(n);
                if (r != null) {
                    r.totalTime = d.getLong("exec_time");
                }
            });
        } catch (JSONException e) {
            throw new OrchestratorError("Invalid runtime report: " + e.getMessage());
        }
    }

    private Map<String, JSONObject> parseData(JSONObject runtimeData) {
        Map<String, JSONObject> runtime = new HashMap<>();
        JSONArray containersArray = runtimeData.getJSONObject("DPERuntime")
                                               .getJSONArray("containers");
        for (int i = 0; i < containersArray.length(); i++) {
            JSONArray servicesArray = containersArray.getJSONObject(i)
                                                     .getJSONArray("services");
            for (int j = 0; j < servicesArray.length(); j++) {
                JSONObject data = servicesArray.getJSONObject(j);
                String name = data.getString("name");
                runtime.put(name, data);
            }
        }
        validateData(runtime);
        return runtime;
    }

    private void validateData(Map<String, JSONObject> runtime) {
        for (String service : runtimeStats.keySet()) {
            JSONObject json = runtime.get(service);
            if (json == null) {
                throw new OrchestratorError("Invalid runtime report: missing " + service);
            }
        }
    }

    long time(String key) {
        Runtime r = runtimeStats.get(key);
        if (r != null) {
            return r.totalTime - r.initialTime;
        }
        throw new OrchestratorError("Invalid runtime report: missing " + key);
    }


    private static class Runtime {
        long initialTime = 0;
        long totalTime = 0;
    }
}
