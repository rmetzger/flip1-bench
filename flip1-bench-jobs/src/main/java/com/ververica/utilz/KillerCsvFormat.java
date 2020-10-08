package com.ververica.utilz;

import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class KillerCsvFormat<T extends Tuple> extends CsvOutputFormat<T> {

    private final KillerClient killerClient;

    public KillerCsvFormat(Path outputPath, String recordDelimiter, String fieldDelimiter, String killerRpcEndpoint) {
        super(outputPath, recordDelimiter, fieldDelimiter);
        killerClient = new KillerClient(killerRpcEndpoint);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        try {
            killerClient.open(getRuntimeContext());
        } catch (Exception e) {
            throw new IOException("Sorry for wrapping", e);
        }
    }

    @Override
    public void writeRecord(T element) throws IOException {
        super.writeRecord(element);
        try {
            killerClient.maybeFail();
        } catch (Throwable e) {
            throw new IOException("Sorry for wrapping", e);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            killerClient.close();
        } catch (Throwable e) {
            throw new IOException("Sorry for wrapping", e);
        }
    }
}
