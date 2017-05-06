import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.CoderUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.NoSuchElementException;

/**
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class CsvWithHeaderFileSource extends FileBasedSource<String> {
    private static final int DEFAULT_MIN_BUNDLE_SIZE = 8 * 1024;

    public static CsvWithHeaderFileSource from(String fileOrPatternSpec) {
        return new CsvWithHeaderFileSource(
                fileOrPatternSpec,
                DEFAULT_MIN_BUNDLE_SIZE);
    }

    /**
     * Do not allow the file to be split so that we can stream through it.
     */
    protected boolean isSplittable() throws Exception {
        return false;
    }

    @Override
    protected FileBasedSource<String> createForSubrangeOfFile(
            final String fileName,
            final long start,
            final long end) {
        return new CsvWithHeaderFileSource(
                fileName,
                getMinBundleSize(),
                start,
                end);
    }

    private CsvWithHeaderFileSource(
            final String fileOrPattern,
            final long minBundleSize,
            final long startOffset,
            final long endOffset) {
        super(fileOrPattern, minBundleSize, startOffset, endOffset);
    }

    private CsvWithHeaderFileSource(
            final String fileOrPattern,
            final long minBundleSize) {
        super(fileOrPattern, minBundleSize);
    }


    @Override
    protected FileBasedReader<String> createSingleFileReader(PipelineOptions options) {
        return new CsvWithHeaderFileBasedReader(this);
    }


    @Override
    public Coder<String> getDefaultOutputCoder() {
        return StringUtf8Coder.of();
    }

    private static class CsvWithHeaderFileBasedReader extends FileBasedReader<String> {
        // eases text reading
        private LineReader lineReader;

        // flag that the header has been read
        private boolean readingStarted = false;

        private String[] header;

        private String currentRecord;

        CsvWithHeaderFileBasedReader(final CsvWithHeaderFileSource source) {
            super(source);
            // do setup here
        }

        @Override
        protected void startReading(final ReadableByteChannel channel)
                throws IOException {
            lineReader = new LineReader(channel);

            if (lineReader.readNextLine()) {
                final String headerLine = lineReader.getCurrent().trim();
                header = headerLine.split(",");
                readingStarted = true;
            }
        }

        @Override
        protected boolean readNextRecord() throws IOException {
            if (!lineReader.readNextLine()) {
                return false;
            }

            final String line = lineReader.getCurrent();
            final String[] data = line.split(",");

            // assumes all lines are valid
            final StringBuilder record = new StringBuilder();
            for (int i = 0; i < header.length; i++) {
                record.append(header[i]).append(":").append(data[i]).append(", ");
            }

            currentRecord = record.toString();
            return true;
        }

        @Override
        protected boolean isAtSplitPoint() {
            // Every record is at a split point.
            return true;
        }

        @Override
        protected long getCurrentOffset() {
            return lineReader.getCurrentLineStart();
        }

        @Override
        public String getCurrent() throws NoSuchElementException {
            if (!readingStarted) {
                throw new NoSuchElementException();
            }

            return currentRecord;
        }
    }

    /**
     * Utility class that helps reading lines of text until eof.
     */
    private static class LineReader {
        private ReadableByteChannel channel = null;
        private long nextLineStart = 0;
        private long currentLineStart = 0;
        private final ByteBuffer buf;
        private static final int BUF_SIZE = 1024;
        private String currentValue = null;

        public LineReader(final ReadableByteChannel channel)
                throws IOException {
            buf = ByteBuffer.allocate(BUF_SIZE);
            buf.flip();

            boolean removeLine = false;
            // If we are not at the beginning of a line, we should ignore the current line.
            if (channel instanceof SeekableByteChannel) {
                SeekableByteChannel seekChannel = (SeekableByteChannel) channel;
                if (seekChannel.position() > 0) {
                    // Start from one character back and read till we find a new line.
                    seekChannel.position(seekChannel.position() - 1);
                    removeLine = true;
                }
                nextLineStart = seekChannel.position();
            }

            this.channel = channel;
            if (removeLine) {
                nextLineStart += readNextLine(new ByteArrayOutputStream());
            }
        }

        private int readNextLine(final ByteArrayOutputStream out)
                throws IOException {
            int byteCount = 0;

            while (true) {
                if (!buf.hasRemaining()) {
                    buf.clear();
                    int read = channel.read(buf);
                    if (read < 0) {
                        break;
                    }
                    buf.flip();
                }

                byte b = buf.get();
                byteCount++;

                if (b == '\n') {
                    break;
                }
                out.write(b);
            }

            return byteCount;
        }

        public boolean readNextLine() throws IOException {
            currentLineStart = nextLineStart;

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            int offsetAdjustment = readNextLine(buf);
            if (offsetAdjustment == 0) {
                // EOF
                return false;
            }
            nextLineStart += offsetAdjustment;
            // When running on Windows, each line obtained from 'readNextLine()' will end with a '\r'
            // since we use '\n' as the line boundary of the reader. So we trim it off here.
            currentValue = CoderUtils.decodeFromByteArray(StringUtf8Coder.of(), buf.toByteArray()).trim();
            return true;
        }

        public String getCurrent() {
            return currentValue;
        }

        public long getCurrentLineStart() {
            return currentLineStart;
        }
    }
}