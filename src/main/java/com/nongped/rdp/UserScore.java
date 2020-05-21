package com.nongped.rdp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;


public class UserScore {

    @DefaultCoder(AvroCoder.class)
    public static class GameActionInfo {
        @Nullable
        String user;
        @Nullable
        String team;
        @Nullable
        Integer score;
        @Nullable
        Long timestamp;

        public GameActionInfo() {
        }

        public GameActionInfo(@Nullable String user,
                              @Nullable String team,
                              @Nullable Integer score,
                              @Nullable Long timestamp) {
            this.user = user;
            this.team = team;
            this.score = score;
            this.timestamp = timestamp;
        }

        @Nullable
        public String getUser() {
            return user;
        }

        public void setUser(@Nullable String user) {
            this.user = user;
        }

        @Nullable
        public String getTeam() {
            return team;
        }

        public void setTeam(@Nullable String team) {
            this.team = team;
        }

        @Nullable
        public Integer getScore() {
            return score;
        }

        public void setScore(@Nullable Integer score) {
            this.score = score;
        }

        @Nullable
        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(@Nullable Long timestamp) {
            this.timestamp = timestamp;
        }

        public String getKey(String keyName) {
            return "team".equals(keyName) ? this.team : this.user;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;

            GameActionInfo gameActionInfo = (GameActionInfo) obj;

            if (!this.getUser().equals(gameActionInfo.getUser())) return false;
            if (!this.getTeam().equals(gameActionInfo.getTeam())) return false;
            if (!this.getScore().equals(gameActionInfo.getScore())) return false;
            return this.getTimestamp().equals(gameActionInfo.getTimestamp());
        }

        @Override
        public int hashCode() {
            return Objects.hash(user, team, score);
        }
    }

    /**
     * Parses the raw game event info into GameActionInfo object. Each event
     * line has the following format: username, teamname, score, timestamp_in_ms, readable_time e.g.:
     * user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224 The human-readable
     */
    public static class ParseEventFn extends DoFn<String, GameActionInfo> {

        /**
         * Log and count parse error
         */
        private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
        private final Counter numParseErrors = Metrics.counter("main", "ParseError");

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] components = c.element().split(",", -1);
            try {
                String user = components[0].trim();
                String team = components[1].trim();
                Integer score = Integer.parseInt(components[2].trim());
                Long timestamp = Long.parseLong(components[3].trim());
                GameActionInfo gameActionInfo = new GameActionInfo(user, team, score, timestamp);
                c.output(gameActionInfo);
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                numParseErrors.inc();
                LOG.info(String.format("Parse error on %s, %s", c.element(), e.getMessage()));
            }
        }
    }

    /**
     * A transform to extract key/score information from GameActionInfo, and sum the scores.  The
     * constructor arg determines whether 'team' or 'user' info is extracted.
     */
    public static class ExtractAndSumScore extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {

        private final String field;

        public ExtractAndSumScore(String field) {
            this.field = field;
        }

        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> gameActionInfo) {
            return gameActionInfo
                    .apply(
                            MapElements
                                    .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                    .via((GameActionInfo gInfo) -> KV.of(gInfo.getKey(field), gInfo.getScore())))
                    .apply(Sum.integersPerKey());
        }
    }

    public interface Options extends PipelineOptions {

        /**
         * The default maps to two large Google Cloud Storage files (each ~12GB) holding two subsequent
         * day's worth (roughly) of data.
         * Note: You may want to use a small sample dataset to test it locally/quickly : gs://apache-beam-samples/game/small/gaming_data.csv
         * You can also download it via the command line gsutil cp gs://apache-beam-samples/game/small/gaming_data.csv ./destination_folder/gaming_data.csv
         */
        @Description("Path to the data file(s) containing game data.")
        @Default.String("gs://apache-beam-samples/game/small/gaming_data.csv")
        String getInput();

        void setInput(String value);


        @Description("Path of the file to write to.")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Integer>, String> {
        @Override
        public String apply(KV<String, Integer> input) {
            return String.format("%s: %d", input.getKey(), input.getValue());
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Start");
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read input CSV from GCS", TextIO.read().from(options.getInput()))
                .apply("Parse Game Event", ParDo.of(new ParseEventFn()))
                .apply("Extract and Sum Scores", new ExtractAndSumScore("user"))
                .apply("Format into text", MapElements.via(new FormatAsTextFn()))
                .apply("Write Scores", TextIO.write().to(options.getOutput()));

        pipeline.run().waitUntilFinish();
    }
}
