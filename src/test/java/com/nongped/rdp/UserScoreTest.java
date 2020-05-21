package com.nongped.rdp;

import com.nongped.rdp.UserScore.GameActionInfo;
import com.nongped.rdp.UserScore.ParseEventFn;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;


@RunWith(JUnit4.class)
public class UserScoreTest {

    @Test
    public void hasCode_ReturnCorrectValue() throws Exception {
        // Arrange
        Long currentTimeStamp = DateTime.now().getMillis();
        GameActionInfo gameActionInfoA = new GameActionInfo("wdaimongkol", "nongped", 95, currentTimeStamp);
        GameActionInfo gameActionInfoB = new GameActionInfo("wdaimongkol", "nongped", 95, currentTimeStamp);

        // Act
        int hashCodeA = gameActionInfoA.hashCode();
        int hashCodeB = gameActionInfoB.hashCode();

        // Assert
        Assert.assertEquals(hashCodeA, hashCodeB);
    }

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(ValidatesRunner.class)
    public void ParseEventFn() throws Exception {
        // Arrange
        final String[] WORDS_ARRAY = new String[]{
                "user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224"
        };
        final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
        PCollection<String> input = pipeline.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());
        GameActionInfo gameActionInfoA = new GameActionInfo("user2_AsparagusPig", "AsparagusPig", 10, 1445230923951L);

        // Act
        PCollection<GameActionInfo> output = input.apply(ParDo.of(new ParseEventFn()));

        // Assert
        PAssert.that(output).containsInAnyOrder(gameActionInfoA);
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void ExtractAndSumScore() throws Exception {
        // Arrange
        String key = "";
        String expectedTeam = "AsparagusPig";
        final GameActionInfo[] GAME_INFOS = new GameActionInfo[]{
                new GameActionInfo("user1_AsparagusPig", expectedTeam, 50, 1445230923959L),
                new GameActionInfo("user1_AsparagusPig", expectedTeam, 20, 1445230923959L),
                new GameActionInfo("user2_AsparagusPig", expectedTeam, 10, 1445230923951L),
                new GameActionInfo("user2_AsparagusPig", expectedTeam, 20, 1445230923959L)
        };
        PCollection<GameActionInfo> input = pipeline.apply(Create.of(Arrays.asList(GAME_INFOS)));
        List<KV<String, Integer>> expected = Arrays.asList(KV.of("user2_AsparagusPig", 30), KV.of("user1_AsparagusPig", 70));

        // Act
        PCollection<KV<String, Integer>> output = input.apply(new UserScore.ExtractAndSumScore(key));

        // Assert
        PAssert.that(output).containsInAnyOrder(expected);
        pipeline.run().waitUntilFinish();
    }
}