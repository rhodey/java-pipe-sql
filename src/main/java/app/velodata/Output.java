package app.velodata;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.concurrent.LinkedBlockingDeque;

public class Output implements Runnable {

    private static final int BUFFER_SIZE = 1024 * 1024;
    private final LinkedBlockingDeque<String> output;
    private final BufferedWriter stderr;
    private final BufferedWriter stdout;

    public Output(LinkedBlockingDeque<String> output) {
        this.stderr = new BufferedWriter(new OutputStreamWriter(System.err), BUFFER_SIZE);
        this.stdout = new BufferedWriter(new OutputStreamWriter(System.out), BUFFER_SIZE);
        this.output = output;
    }

    @Override
    public void run() {
        try {
            boolean errHasData = false;
            boolean outHasData = false;

            while (true) {

                if (output.isEmpty()) {
                    if (errHasData) { stderr.flush(); }
                    if (outHasData) { stdout.flush(); }
                    errHasData = outHasData = false;
                }

                String next = output.take();
                if (next.startsWith("e:") || next.startsWith("i:")) {
                    stderr.write(next.substring(2));
                    stderr.newLine();
                    errHasData = true;
                } else {
                    stdout.write(next.substring(2));
                    stdout.newLine();
                    outHasData = true;
                }

            }
        } catch (Exception e) {
            String error = e.getMessage();
            if (error == null) { error = e.getClass().getName(); }
            error = error.replace("\n", " ").replace(",", " ");
            System.err.println("\n*,exiting output loop: " + error);
        }
    }

}