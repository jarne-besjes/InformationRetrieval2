package org.example;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import net.sourceforge.argparse4j.impl.Arguments;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Main {

    static Analyzer getAnalyzer() {
        // The used analyzer can be changed here!
        return new StandardAnalyzer();
    }

    private static void add_arguments_to_parser(ArgumentParser parser) {
        parser.addArgument("--query", "-q")
                .help("Query to search for")
                .type(String.class);
        parser.addArgument("-n")
                .help("Number of results to return")
                .type(Integer.class);
        parser.addArgument("--no-index")
                .help("Do not index the documents")
                .action(Arguments.storeTrue());
        parser.addArgument("--docs-folder")
                .help("Folder containing the documents to index")
                .type(String.class);
        parser.addArgument("--mode", "-m")
                .help("Mode to run the program in")
                .choices("query", "bench", "gen_csv")
                .setDefault("query");
    }

    static void generateIndex(Path indexPath, Path corpusPath, Analyzer analyzer) throws IOException {
        var indexDir = FSDirectory.open(indexPath);
        var writerConfig = new IndexWriterConfig(analyzer);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        try (var indexWriter = new IndexWriter(indexDir, writerConfig)) {
            try (Stream<Path> paths = Files.walk(corpusPath)) {
                paths
                        .filter(Files::isRegularFile)
                        .filter(path -> path.toString().endsWith(".txt"))
                        .forEach(path -> {
                            try {
                                var content = Files.readString(path);
                                var fileName = path.getFileName().toString();
                                var docIdStr = fileName.substring(fileName.indexOf("_") + 1, fileName.indexOf("."));
                                var docId = Integer.parseInt(docIdStr);
                                var doc = new Document();
                                doc.add(new TextField("title", fileName, Field.Store.YES));
                                doc.add(new IntField("docId", docId, Field.Store.YES));
                                doc.add(new TextField("content", content, Field.Store.NO));
                                indexWriter.addDocument(doc);
                            } catch (IOException e) {
                                System.err.println("Error reading file " + path + ": " + e.getMessage());
                            }
                        });
            }
        }
    }

    static void query(Path indexPath, Analyzer analyzer) throws IOException, ParseException {
        var queryStr = "does xpress bet charge to deposit money in your account";
        var query = new QueryParser("content", analyzer).parse(queryStr);

        var indexDir = FSDirectory.open(indexPath);

        var indexReader = DirectoryReader.open(indexDir);
        var searcher = new IndexSearcher(indexReader);
        var topDocs = searcher.search(query, 10);
        ScoreDoc[] hits = topDocs.scoreDocs;
        var storedFields = searcher.storedFields();

        for (var hit : hits) {
            var hitDoc = storedFields.document(hit.doc);
            System.out.println("title: " + hitDoc.get("title") + ", doc number: " + hit.doc);
        }

    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newFor("Main").build()
                .usage("Main [OPTIONS]")
                .defaultHelp(true)
                .description("Index and search documents");

        add_arguments_to_parser(parser);

        Namespace res = parser.parseArgsOrFail(args);

        var docs_path = res.get("docs_folder").toString();
        if (docs_path == null) {
            System.err.println("Please provide the path to the documents folder");
            System.exit(1);
        }
        // strip / of path and add _index
        var indexPath = Paths.get(docs_path.toString().replaceAll("/+$", "") + "_index");
        if (!Files.exists(indexPath)) {
            generateIndex(indexPath, Paths.get(docs_path), getAnalyzer());
        }

        if (res.get("mode").equals("query")) {
            query(indexPath, getAnalyzer());
        } else if (res.get("mode").equals("bench")) {
            runBenchmark(indexPath, getAnalyzer());
        } else if (res.get("mode").equals("gen_csv")) {
            // generate_csv(indexPath, getAnalyzer());
        }
    }

    private static void runBenchmark(Path indexPath, Analyzer analyzer) throws IOException, CsvException, ParseException {
        var queriesReader = new CSVReaderBuilder(new FileReader("queries.tsv")).withSkipLines(1).withCSVParser(new CSVParserBuilder().withSeparator('\t').build()).build();
        var queries = queriesReader.readAll();
        var resultsReader = new CSVReaderBuilder(new FileReader("expected_results.csv")).withSkipLines(1).withCSVParser(new CSVParserBuilder().withSeparator(',').build()).build();
        var expectedResults = resultsReader.readAll();

        var indexDir = FSDirectory.open(indexPath);
        var indexReader = DirectoryReader.open(indexDir);
        var searcher = new IndexSearcher(indexReader);

        List<List<Float>> avgPrecisions = new ArrayList<>();
        List<List<Float>> avgRecalls = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            avgPrecisions.add(new ArrayList<>());
            avgRecalls.add(new ArrayList<>());
        }

        for (var query : queries) {
            var queryNr = query[0];
            var queryStr = QueryParser.escape(query[1]);

            var queryObj = new QueryParser("content", analyzer).parse(queryStr);
            var topDocs = searcher.search(queryObj, 10);

            var docTitles = getDocTitles(topDocs, searcher);

            var docNrs = Arrays.stream(docTitles)
                    .map(title -> title.substring(title.indexOf("_") + 1, title.indexOf(".")))
                    .toArray(String[]::new);

            var expected = expectedResults.stream()
                    .filter(r -> r[0].equals(queryNr))
                    .map(row -> row[1]).toList();

            int[] ks = {1, 3, 5, 10};
            for (int ki = 0; ki < ks.length; ki++) {
                var k = ks[ki];

                var precision = calculatePrecisionAtK(docNrs, expected, k);
                var recall = calculateRecallAtK(docNrs, expected, k);
                avgPrecisions.get(ki).add(precision);
                avgRecalls.get(ki).add(recall);
            }
        }

        var meanAvgPrecisions = avgPrecisions.stream()
                .map(precisions -> precisions.stream()
                        .mapToDouble(Float::doubleValue)
                        .average()
                        .orElse(0))
                .toList();
        var meanAvgRecalls = avgRecalls.stream()
                .map(recalls -> recalls.stream()
                        .mapToDouble(Float::doubleValue)
                        .average()
                        .orElse(0))
                .toList();

        System.out.println("k,mean_avg_precision,mean_avg_recall");
        for (int i = 0; i < 4; i++) {
            System.out.println((i + 1) + "," + meanAvgPrecisions.get(i) + "," + meanAvgRecalls.get(i));
        }
    }

    private static String[] getDocTitles(TopDocs topDocs, IndexSearcher searcher) throws IOException {
        ScoreDoc[] retrieved = topDocs.scoreDocs;
        var storedFields = searcher.storedFields();
        var docTitles = new String[retrieved.length];
        for (int i = 0; i < retrieved.length; i++) {
            var hitDoc = storedFields.document(retrieved[i].doc);
            docTitles[i] = hitDoc.get("title");
        }
        return docTitles;
    }


    private static float calculateRecallAtK(String[] retrieved, List<String> expected, int k) {
        int count = 0;
        float sum = 0;

        for (int i = 1; i < k + 1; i++) {
            if (i <= retrieved.length && expected.contains(retrieved[i - 1])) {
                count++;
                sum += (float) count / expected.size();
            }
        }

        if (count == 0) {
            return 0;
        }
        return sum / count;
    }

    private static float calculatePrecisionAtK(String[] retrieved, List<String> expected, int k) {
        int count = 0;
        float sum = 0;
        for (int i = 1; i < k + 1; i++) {
            if (i <= retrieved.length && expected.contains(retrieved[i - 1])) {
                count++;
                sum += (float) count / i;
            }
        }
        if (count == 0) {
            return 0;
        }
        return sum / count;
    }
}