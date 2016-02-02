package org.jlab.clas.std.orchestrators.errors;

public class OrchestratorError extends RuntimeException {

    private static final long serialVersionUID = -5459481851420223735L;

    public OrchestratorError() {
    }

    public OrchestratorError(String message) {
        super(message);
    }

    public OrchestratorError(Throwable cause) {
        super(cause);
    }

    public OrchestratorError(String message, Throwable cause) {
        super(message, cause);
    }

}
