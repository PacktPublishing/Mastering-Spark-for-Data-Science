package io.gzet.timeseries.twitter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import twitter4j.*;
import twitter4j.Twitter;
import twitter4j.auth.AccessToken;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FetchTweets {

    private static final int TWEETS_PER_QUERY = 90;

    public static void main(String[] args) throws TwitterException, IOException, InterruptedException {

        String propPath = System.getProperty("twitter.properties");
        if (StringUtils.isEmpty(propPath)) {
            System.err.println("Missing properties file");
            System.err.println("Usage : <filter> <fromDate:YYYY-MM-DD> <toDate:YYYY-MM-DD> <fileName> -Dtwitter.properties=/path/to/properties");
            System.exit(1);
        }

        if (args.length < 4) {
            System.err.println("Missing arguments");
            System.err.println("Usage : <filter> <fromDate:YYYY-MM-DD> <toDate:YYYY-MM-DD> <fileName> -Dtwitter.properties=/path/to/properties");
            System.exit(1);
        }

        Properties properties = new Properties();
        FileInputStream in = new FileInputStream(propPath);
        properties.load(in);
        in.close();

        String apiKey = properties.getProperty("twitter.api.key").trim();
        String apiSecret = properties.getProperty("twitter.api.secret").trim();
        String accessToken = properties.getProperty("twitter.token").trim();
        String accessTokenSecret = properties.getProperty("twitter.token.secret").trim();
        if (StringUtils.isEmpty(apiKey) || StringUtils.isEmpty(apiKey) || StringUtils.isEmpty(apiKey) || StringUtils.isEmpty(apiKey)) {
            System.err.println("Invalid configuration");
            System.err.println("\t - twitter.token = XXXXXX");
            System.err.println("\t - twitter.token.secret = XXXXXX");
            System.err.println("\t - twitter.api.key = XXXXXX");
            System.err.println("\t - twitter.api.secret = XXXXXX");
            System.exit(1);
        }

        new FetchTweets().retrieve(apiKey, apiSecret, accessToken, accessTokenSecret, args[0], args[1], args[2], args[3]);
    }

    public void retrieve(String apiKey, String apiSecret, String accessToken, String accessTokenSecret, String filter, String fromDate, String toDate, String fileName) throws TwitterException, IOException, InterruptedException {

        long start = new Date().getTime();



        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setOAuthConsumerKey(apiKey);
        builder.setOAuthConsumerSecret(apiSecret);
        Configuration configuration = builder.build();

        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        Twitter twitter = new TwitterFactory(configuration).getInstance(token);

        Map<String, RateLimitStatus> rls = twitter.getRateLimitStatus("search");

        Gson gson = new GsonBuilder().create();
        PrintWriter writer = new PrintWriter(fileName, "UTF-8");
        RateLimitStatus strl = rls.get("/search/tweets");

        int totalTweets = 0;
        long maxID = -1;
        for (int i = 0; i < 400; i++) {

            // throttling
            if (strl.getRemaining() == 0) {
                Thread.sleep(strl.getSecondsUntilReset() * 1000L);
            }

            Query q = new Query(filter);
            q.setSince(fromDate);
            q.setUntil(toDate);
            q.setCount(TWEETS_PER_QUERY);

            // paging
            if (maxID != -1) {
                q.setMaxId(maxID - 1);
            }

            QueryResult r = twitter.search(q);
            for (Status s: r.getTweets()) {
                totalTweets++;
                if (maxID == -1 || s.getId() < maxID) {
                    maxID = s.getId();
                }
                writer.println(gson.toJson(s));
            }
            strl = r.getRateLimitStatus();
        }

        long duration = new Date().getTime() - start;
        System.out.printf("A total of %d tweets retrieved\n", totalTweets);
        System.out.println("Process took " + ((duration / 1000) / 60) + " minutes");
        writer.close();
    }

}
