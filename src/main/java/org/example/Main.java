package org.example;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


public class Main {
    static void generateIndex(Path indexPath, Path corpusPath, Analyzer analyzer) throws IOException {
        var indexDir = FSDirectory.open(indexPath);
        var writerConfig = new IndexWriterConfig(analyzer);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        try( var indexWriter = new IndexWriter(indexDir, writerConfig) ) {
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
        var queryStr = "types of road hugger tires";
        var query = new QueryParser("content", analyzer).parse(queryStr);

        var indexDir = FSDirectory.open(indexPath);

        var indexReader = DirectoryReader.open(indexDir);
        var searcher = new IndexSearcher(indexReader);
        var topDocs = searcher.search(query, 10);
        ScoreDoc[] hits = topDocs.scoreDocs;
        var storedFields = searcher.storedFields();

        for(var hit : hits) {
            var hitDoc = storedFields.document(hit.doc);
            System.out.println("title: " + hitDoc.get("title") + ", doc number: " + hit.doc);
        }

    }

    public static void main(String[] args) throws Exception {
        var indexPath = Paths.get("full_docs_small_index");
        var corpusPath = Paths.get("full_docs_small");
        var analyzer = new StandardAnalyzer();
        generateIndex(indexPath, corpusPath, analyzer);
        query(indexPath, analyzer);
    }
}