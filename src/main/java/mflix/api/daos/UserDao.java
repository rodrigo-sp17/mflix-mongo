package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.swing.event.DocumentEvent;
import java.text.MessageFormat;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.ne;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;

    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());

        sessionsCollection = db.getCollection("sessions", Session.class)
                .withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!
        usersCollection.insertOne(user);
        return true;
        //TODO > Ticket: Handling Errors - make sure to only add new users
        // and not users that already exist.

    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        Bson updateSession = Updates.combine(
                Updates.set("jwt", jwt), Updates.setOnInsert("user_id", userId)
        );

        UpdateOptions options = new UpdateOptions();
        options.upsert(true);

        Bson query = Filters.eq("user_id", userId);

        final UpdateResult result = sessionsCollection
                .updateOne(query, updateSession, options);

        if (result.getUpsertedId() == null && result.getModifiedCount() == 0) {
            log.error("Error creating user Session");
            return false;
        } else {
            return true;
        }

        //TODO > Ticket: Handling Errors - implement a safeguard against
        // creating a session with the same jwt token.
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        User user = null;

        Bson query = new Document("email", email);
        user = usersCollection
                .find(query)
                .iterator().tryNext();

        return user;
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {

        Bson query = new Document("user_id", userId);

        final Session session = sessionsCollection.find(query).iterator().tryNext();

        return session;
    }

    public boolean deleteUserSessions(String userId) {
        Bson query = eq("user_id", userId);

        Long deletedCount = sessionsCollection.deleteMany(query).getDeletedCount();

        return deletedCount > 0;
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        deleteUserSessions(email);

        User deleted = usersCollection.findOneAndDelete(new Document("email", email));

        return deleted != null;
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions.
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
        // be updated.
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions when updating an entry.
        return false;
    }
}
