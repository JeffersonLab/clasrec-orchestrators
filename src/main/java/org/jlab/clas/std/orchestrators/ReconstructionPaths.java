package org.jlab.clas.std.orchestrators;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class ReconstructionPaths {

    static final String DATA_DIR = System.getenv("CLARA_HOME") + File.separator + "data";
    static final String CACHE_DIR = "/mss/hallb/exp/raw";
    static final String INPUT_DIR = DATA_DIR + File.separator + "in";
    static final String OUTPUT_DIR = DATA_DIR + File.separator + "out";
    static final String STAGE_DIR = File.separator + "scratch";

    final String inputDir;
    final String outputDir;
    final String stageDir;

    final List<ReconstructionFile> allFiles;

    ReconstructionPaths(String inputFile, String outputFile) {
        Path inputPath = Paths.get(inputFile);
        Path outputPath = Paths.get(outputFile);

        String inputName = inputPath.getFileName().toString();
        String outputName = outputPath.getFileName().toString();

        this.inputDir = getParent(inputPath);
        this.outputDir = getParent(outputPath);
        this.stageDir = STAGE_DIR;

        this.allFiles = Arrays.asList(new ReconstructionFile(inputName, outputName));
    }

    ReconstructionPaths(List<String> inputFiles,
                        String inputDir,
                        String outputDir,
                        String stageDir) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        this.stageDir = stageDir;
        this.allFiles = inputFiles.stream()
                                  .map(f -> new ReconstructionFile(f, "out_" + f))
                                  .collect(Collectors.toList());
    }

    private String getParent(Path file) {
        Path parent = file.getParent();
        if (parent == null) {
            return Paths.get("").toAbsolutePath().toString();
        } else {
            return parent.toString();
        }
    }

    String inputFilePath(ReconstructionFile recFile) {
        return inputDir + File.separator + recFile.inputName;
    }

    String outputFilePath(ReconstructionFile recFile) {
        return outputDir + File.separator + recFile.outputName;
    }

    String stageInputFilePath(ReconstructionFile recFile) {
        return stageDir + File.separator + recFile.inputName;
    }

    String stageOutputFilePath(ReconstructionFile recFile) {
        return stageDir + File.separator + recFile.outputName;
    }

    int numFiles() {
        return allFiles.size();
    }
}
