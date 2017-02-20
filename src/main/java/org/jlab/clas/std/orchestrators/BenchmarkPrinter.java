package org.jlab.clas.std.orchestrators;

class BenchmarkPrinter {

    private final Benchmark benchmark;

    private long totalTime = 0;
    private long totalRequests = 0;

    BenchmarkPrinter(Benchmark benchmark, long totalRequests) {
        this.benchmark = benchmark;
        this.totalRequests = totalRequests;
    }

    void printBenchmark(ReconstructionSetup setup) {
        Logging.info("%nBenchmark results:");
        ServiceInfo reader = setup.ioServices.get("reader");
        ServiceInfo writer = setup.ioServices.get("writer");
        printService(reader, "READER");
        for (ServiceInfo service : setup.recChain) {
            printService(service, service.name);
        }
        printService(writer, "WRITER");
        printTotal();
    }

    private void printService(ServiceInfo service, String label) {
        long time = benchmark.time(service);
        totalTime += time;
        print(label, time, totalRequests);
    }

    private void printTotal() {
        print("TOTAL", totalTime, totalRequests);
    }

    private void print(String name, long time, long requests) {
        double timePerEvent = (time / requests) / 1e3;
        Logging.info("  %-9s  %5d events  " +
                     "  total time = %7.2f s  " +
                     "  average event time = %6.2f ms",
                     name, requests, time / 1e6, timePerEvent);

    }
}
