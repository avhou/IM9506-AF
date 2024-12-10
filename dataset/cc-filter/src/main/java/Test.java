import lombok.SneakyThrows;
import org.jsoup.Jsoup;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.WarcFilter;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcResponse;
import org.netpreserve.jwarc.WarcWriter;
import picocli.CommandLine;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.StreamSupport;

public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        Tar tar = new Tar();
        new CommandLine(tar).parseArgs(args);
        CommandLine.usage(tar, System.out);
        String expression = "warc-target-uri =~ \"(?i).*VOORUIT\\.org.*\"";

        try (WarcReader reader = new WarcReader(FileChannel.open(Paths.get("/Users/alexander/ou/IM9506-AF/dataset/warc-vooruit.gz")));
                WarcWriter writer = new WarcWriter(FileChannel.open(Paths.get("/Users/alexander/ou/IM9506-AF/dataset/warc-vooruit-filtered.gz"), StandardOpenOption.CREATE, StandardOpenOption.WRITE))) {
            reader.records()
                  .filter(WarcFilter.compile(expression))
                  .limit(100)
                  .forEach(record -> {
                        processRecord(record);
                        writeRecord(record, writer);
                  });
//            for (WarcRecord record : reader) {
//                if (record instanceof WarcResponse && record.contentType().base().equals(MediaType.HTTP)) {
//                    WarcResponse response = (WarcResponse) record;
//                    response.http().headers().first("Content-Type").ifPresent(System.out::println);
//                    System.out.println(response.http().status() + " " + response.target());
//                    System.out.print(Jsoup.parse(response.http().bodyDecoded().stream(), "UTF-8", response.target().toString()).head().text());
//                }
//            }
        }
    }

    @SneakyThrows
    private static void processRecord(WarcRecord record) {
        if (record instanceof WarcResponse && record.contentType().base().equals(MediaType.HTTP)) {
            WarcResponse response = (WarcResponse) record;
            response.http();
//            response.http().headers().first("Content-Type").ifPresent(System.out::println);
//            System.out.println(response.http().status() + " " + response.target());
            System.out.println(response.headers().first("WARC-Target-URI"));
            System.out.println(Jsoup.parse(response.http().bodyDecoded().stream(), determineCharset(response), response.target().toString()).body().text());
        }
    }

    @SneakyThrows
    private static void writeRecord(WarcRecord record, WarcWriter writer) {
        writer.write(record);
    }

    @SneakyThrows
    private static String determineCharset(WarcResponse response) {
        return response.http().headers().first("Content-Type")
                .map(contentType -> contentType.split(";"))
                .filter(parts -> parts.length > 1)
                .map(parts -> parts[1].split("="))
                .filter(parts -> parts.length > 1)
                .map(parts -> parts[1])
                .orElse("UTF-8");
    }
}
