package GetStarted;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import org.bson.conversions.Bson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

import java.util.*;

/**
 * Simple application that shows how to use Azure Cosmos DB for MongoDB API and Azure Redis Cache via Jedis,
 * in a Java application.
 */
public class Program {

    static MongoClient gMongoClient = null;
    static Jedis gJedisClient = null;

    public static MongoClient getClient() {
        if (gMongoClient == null) {
            MongoClientURI uri = new MongoClientURI("MONGO CONNECTION STRING");
            gMongoClient = new MongoClient(uri);
        }
        return gMongoClient;
    }

    public static Jedis getJedisClient() {
        if (gJedisClient == null) {
            JedisShardInfo shardInfo = getCacheInfo();
            gJedisClient = new Jedis(shardInfo);
        }
        return gJedisClient;
    }

    public static JedisShardInfo getCacheInfo() {
        boolean useSsl = true;
        String cacheHostname = "CACHE ADDRESS";
        String cachekey = "CACHE KEY";

        // Connect to the Azure Cache for Redis over the TLS/SSL port using the key.
        JedisShardInfo shardInfo = new JedisShardInfo(cacheHostname, 6380, 4000, useSsl);
        shardInfo.setPassword(cachekey);

        return shardInfo;
    }

    public static void main(String[] args) {
        if (args[0].equals("-U")) {
            GenerateData(5);
        } else if(args[0].equals("-I")) {
            invalidateCache();
        }

        if(!args[0].equals("-U"))
        {
            // precache the clients so the slow startup doesn't affect the timing
            getClient();
            getJedisClient();

            // retrieve two pieces of information from the collection
            // all fixture results from season 2, and all results from seasons where pitch invasions happened
            Bson season2_query = Filters.eq("Season", 2);
            Bson pitch_query = Filters.eq("Fixtures.Special", "Pitch Invasion");

            long startTime = System.nanoTime();
            String json_output = getCachedOrRetrieve(season2_query);
            String pitch_output = getCachedOrRetrieve(pitch_query);
            long endTime = System.nanoTime();

            System.out.println(json_output);
            System.out.println(pitch_output);

            Long time_taken = endTime - startTime;
            System.out.println("Time taken" + time_taken.toString());

            System.out.println("Invalidating cache and repeating query...");

            invalidateCache();

            long startTime2 = System.nanoTime();
            String json_output2 = getCachedOrRetrieve(season2_query);
            String pitch_output2 = getCachedOrRetrieve(pitch_query);
            long endTime2 = System.nanoTime();

            System.out.println(json_output2);
            System.out.println(pitch_output2);

            Long time_taken2 = endTime2 - startTime2;
            System.out.println("Time taken" + time_taken2.toString());
        }

        if (gMongoClient != null) {
            gMongoClient.close();
        }
        if (gJedisClient != null) {
            gJedisClient.close();
        }
    }

    public static void GenerateData(int seasons) {

        MongoClient client = getClient();

        MongoDatabase database = client.getDatabase("db");
        MongoCollection<Document> collection = database.getCollection("results");

        // erase all existing entries
        collection.deleteMany(new BasicDBObject());

        String[] teams = {"Brumlington Wanderers",
                "Excessive Rovers",
                "Losechester City",
                "Spangles United",
                "Gunstown",
                "Cramlington Stanley"
        };

        Random rand = new Random();

        // for each season
        for (int season = 0; season < seasons; season++) {
            Document seasonResults = new Document();
            BasicDBObject sDB = new BasicDBObject();
            ArrayList<BasicDBObject> sEntries = new ArrayList<BasicDBObject>();
            // every combination of teams
            for (int i = 0; i < teams.length; i++) {
                for (int j = i + 1; j < teams.length; j++) {
                    Date now = new Date();

                    int score1 = rand.nextInt(3);
                    int score2 = rand.nextInt(3);

                    BasicDBObject entry = new BasicDBObject();
                    entry.put("Home Team", teams[i]);
                    entry.put("Away Team", teams[j]);
                    entry.put("Home Score", score1);
                    entry.put("Away Score", score2);

                    entry.put("Input Date", now);

                    int specials = rand.nextInt(21);

                    if (specials == 20) {
                        entry.put("Special", "Pitch Invasion");
                    }

                    if (specials > 17 && specials < 20) {
                        entry.put("Special", "Extra Time");
                    }

                    sEntries.add(entry);
                }
            }
            seasonResults.put("Season", season);
            seasonResults.put("Fixtures", sEntries);

            collection.insertOne(seasonResults);
        }

        invalidateCache();

    }

    public static String retrieve(Bson query) {
        MongoClient client = getClient();

        MongoDatabase database = client.getDatabase("db");
        MongoCollection<Document> collection = database.getCollection("results");

        FindIterable<Document> results = collection.find(query);

        MongoCursor<Document> cursor = results.iterator();
        ArrayList<BasicDBObject> list = new ArrayList<BasicDBObject>();
        //ArrayList<String> list = new ArrayList<String>();
        while (cursor.hasNext())
        {
            Document n = cursor.next();
            BasicDBObject dbo = new BasicDBObject(n);
            String f = n.toJson();
            list.add(dbo);
        }

        Document output = new Document();
        output.put("results",list);

        return output.toJson().toString();
    }

    public static void invalidateCache() {
        Jedis jedis = getJedisClient();

        jedis.flushAll();
    }

    public static String getCachedOrRetrieve(Bson query) {
        String q = query.toString();

        Jedis jedis = getJedisClient();

        String result = jedis.get(q);

        if (!jedis.exists(q)) {
            // missed cache result
            result = retrieve(query);
            if (result != null) {
                // successfully found a result in the DB
                jedis.set(q, result);
            }
        }

        return result;
    }
}