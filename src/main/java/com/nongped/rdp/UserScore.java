package com.nongped.rdp;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
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

}
