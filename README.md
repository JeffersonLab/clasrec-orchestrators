# clasrec-orchestrators

**This project is DEPRECATED.
The orchestrators have been merged into CLARA v4.3 standard distribution.**

**Up-to-date information about running CLAS reconstruction
can be found in https://claraweb.jlab.org.**

CLARA orchestrators to process CLAS12 files in a chain of reconstruction
services. The chain should be configured with your selected services.

## Local orchestrator

The `LocalOrchestrator` starts an orchestrator that allows you to quickly test
a reconstruction chain in your local box, by processing a local input file
with a fixed number of threads. A local Clara DPE must be running.

The orchestrator will deploy the reconstruction chain in the local DPE.
Once the chain is deployed, it will request the standard IO reader service
to start sending events to the chain, until all events are processed.
The orchestrator will exit after the file has been reconstructed.

The average reconstruction time per event will be printed in the standard
output, and the reconstructed events will be saved in the specified output
file.

<a href="https://asciinema.org/a/36325" target="_blank"><img src="https://asciinema.org/a/36325.png" width="600"/></a>

## Cloud orchestrator

The `CloudOrchestrator` starts an orchestrator that will process a list
of input files for reconstruction on a set of worker nodes. It will wait for
DPEs to be started on the nodes and it will use them as soon as they are
alive.

For each DPE, the orchestrator will first deploy the standard I/O services and
the user reconstruction services.
Then it will configure the I/O services to process one of the input files.
Finally, it will request to start the reconstruction of the file using all
cores in the node.

Each new DPE will be used for I/O and reconstruction (i.e. each node will be
in charge of reconstructing its own single file). When a DPE finishes a file,
the orchestrator will use it to process the next file in the list.

The orchestrator will exit after all files have been reconstructed.

<a href="https://asciinema.org/a/37351" target="_blank"><img src="https://asciinema.org/a/37351.png" width="600"/></a>

<img src="https://userweb.jlab.org/~smancill/clas12/clas-multinode-orchestrator.png" width="600"/>
