package com.joshua;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

public class CSVIterator implements Iterator<Map<String, String>>, AutoCloseable {

    private CSVParser parser;
    private Iterator<CSVRecord> csvRecordIterator;

    public CSVIterator(File csvFile, char separator) throws IOException, FileNotFoundException {
        CSVFormat csvFormat = CSVFormat.newFormat(separator)
                                .withIgnoreSurroundingSpaces()
                                .withIgnoreEmptyLines()
                                .withEscape('\\' )
                                .withFirstRecordAsHeader()
                                .withQuote('\"')
                                .withNullString(null);

        parser = CSVParser.parse(csvFile, Charset.defaultCharset(), csvFormat);
        csvRecordIterator = parser.iterator();

    }

    @Override
    public Map<String, String> next() {
        CSVRecord record = csvRecordIterator.next();
        return record.toMap();
    }

    @Override
    public boolean hasNext() {
        return csvRecordIterator.hasNext();
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot `remove` on CSVIterator");
    }

    @Override
    public void close() {
        try {
            parser.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
