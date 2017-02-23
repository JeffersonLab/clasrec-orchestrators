package org.jlab.clas.std.orchestrators;

final class ReconstructionOptions {

    static final int DEFAULT_POOLSIZE = 32;
    static final int MAX_NODES = 512;
    static final int MAX_THREADS = 64;

    final boolean useFrontEnd;
    final boolean stageFiles;
    final boolean bulkStage;
    final int poolSize;
    final int maxNodes;
    final int maxThreads;
    final int reportFreq;


    static Builder builder() {
        return new Builder();
    }


    static final class Builder {

        private boolean useFrontEnd = false;
        private boolean stageFiles = false;
        private boolean bulkStage = false;
        private int poolSize = DEFAULT_POOLSIZE;
        private int maxNodes = MAX_NODES;
        private int maxThreads = MAX_THREADS;
        private int reportFreq = 0;

        Builder useFrontEnd() {
            this.useFrontEnd = true;
            return this;
        }

        Builder stageFiles() {
            this.stageFiles = true;
            return this;
        }

        Builder bulkStage() {
            this.bulkStage = true;
            return this;
        }

        Builder withPoolSize(int poolSize) {
            if (poolSize <= 0) {
                throw new IllegalArgumentException("Invalid pool size: " + poolSize);
            }
            this.poolSize = poolSize;
            return this;
        }

        Builder withMaxNodes(int maxNodes) {
            if (maxNodes <= 0) {
                throw new IllegalArgumentException("Invalid max number of nodes: " + maxNodes);
            }
            this.maxNodes = maxNodes;
            return this;
        }

        Builder withMaxThreads(int maxThreads) {
            if (maxThreads <= 0) {
                throw new IllegalArgumentException("Invalid max number of threads: " + maxThreads);
            }
            this.maxThreads = maxThreads;
            return this;
        }

        Builder withReportFrequency(int reportFreq) {
            if (reportFreq <= 0) {
                throw new IllegalArgumentException("Invalid report frequency: " + reportFreq);
            }
            this.reportFreq = reportFreq;
            return this;
        }

        ReconstructionOptions build() {
            return new ReconstructionOptions(this);
        }
    }


    private ReconstructionOptions(Builder builder) {
        this.useFrontEnd = builder.useFrontEnd;
        this.stageFiles = builder.stageFiles || builder.bulkStage;
        this.bulkStage = builder.bulkStage;
        this.poolSize = builder.poolSize;
        this.maxNodes = builder.maxNodes;
        this.maxThreads = builder.maxThreads;
        this.reportFreq = builder.reportFreq;
    }
}
