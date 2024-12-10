import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;

class Tar {
    @Option(names = "-c", description = "create a new archive")
    boolean create;

    @Option(names = { "-f", "--file" }, paramLabel = "ARCHIVE", description = "the archive file")
    File archive;

    @Parameters(paramLabel = "FILE", description = "one or more files to archive")
    File[] files;

    @Option(names = { "-h", "--help" }, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;
}
