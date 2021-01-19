package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Comment;
import mflix.api.models.Critic;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.ObjectIdGenerator;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.codecs.pojo.PropertyModelBuilder;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Component
public class CommentDao extends AbstractMFlixDao {

    public static String COMMENT_COLLECTION = "comments";
    private final Logger log;
    private MongoCollection<Comment> commentCollection;
    private CodecRegistry pojoCodecRegistry;

    @Autowired
    public CommentDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        log = LoggerFactory.getLogger(this.getClass());
        this.db = this.mongoClient.getDatabase(MFLIX_DATABASE);
        this.pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        this.commentCollection =
                db.getCollection(COMMENT_COLLECTION, Comment.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Returns a Comment object that matches the provided id string.
     *
     * @param id - comment identifier
     * @return Comment object corresponding to the identifier value
     */
    public Comment getComment(String id) {
        return commentCollection.find(new Document("_id", new ObjectId(id))).first();
    }

    /**
     * Adds a new Comment to the collection. The equivalent instruction in the mongo shell would be:
     *
     * <p>db.comments.insertOne({comment})
     *
     * <p>
     *
     * @param comment - Comment object.
     * @throw IncorrectDaoOperation if the insert fails, otherwise
     * returns the resulting Comment object.
     */
    public Comment addComment(Comment comment) {

        if (comment.getId() == null) {
            throw new IncorrectDaoOperation("Id can't be null");
        }

        log.info("Going to insert: " + comment.getDate());
        MongoCollection<Document> commentCol = db.getCollection(COMMENT_COLLECTION);

        Document toInsert = new Document("name", comment.getName());
        toInsert.put("_id", new ObjectId(comment.getId()));
        toInsert.put("movie_id", comment.getMovieObjectId());
        toInsert.put("email", comment.getEmail());
        toInsert.put("text", comment.getText());

        // Manual conversion necessary to avoid issues with BsonTimestamp conversion
        if (comment.getDate() == null) {
            toInsert.put("date", new Date(System.currentTimeMillis()));
        } else {
            toInsert.put("date", new Date(comment.getDate().getTime()));
        }

        try {
            commentCol.insertOne(toInsert);
        } catch (MongoWriteException e) {
            log.error("Could not insert comment: " + e.getError().getCategory());
        }

        Document inserted = commentCol.find(Filters.eq("_id", toInsert.get("_id")))
                .first();

        if (inserted == null) {
            throw new IncorrectDaoOperation("Comment insertion failed");
        }

        Comment insertedComment = new Comment();
        insertedComment.setDate(inserted.getDate("date"));
        insertedComment.setText(inserted.getString("text"));
        insertedComment.setEmail(inserted.getString("email"));
        insertedComment.setName(inserted.getString("name"));
        insertedComment.setMovieId(inserted.getObjectId("movie_id").toHexString());
        insertedComment.setId(inserted.getObjectId("_id").toHexString());

        return insertedComment;
        // TODO> Ticket - Handling Errors: Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
    }

    /**
     * Updates the comment text matching commentId and user email. This method would be equivalent to
     * running the following mongo shell command:
     *
     * <p>db.comments.update({_id: commentId}, {$set: { "text": text, date: ISODate() }})
     *
     * <p>
     *
     * @param commentId - comment id string value.
     * @param text      - comment text to be updated.
     * @param email     - user email.
     * @return true if successfully updates the comment text.
     */
    public boolean updateComment(String commentId, String text, String email) {

        Comment toEdit = commentCollection.find(Filters.eq("_id", new ObjectId(commentId)))
                .first();

        if (!toEdit.getEmail().equals(email)) {
            log.warn("Only the original user cant edit his comments");
            return false;
        }

        Bson newText = Updates.set("text", text);
        Bson newDate = Updates.set("date", new Date(System.currentTimeMillis()));
        Bson update = Updates.combine(newText, newDate);

        Long modifiedCount = 0L;
        try {
            modifiedCount = commentCollection
                    .updateOne(Filters.eq("_id", toEdit.getOid()),
                            update)
                    .getModifiedCount();
        } catch (MongoWriteException e) {
            log.error("Could not update comment: " + e.getError().getCategory());
        }

        return modifiedCount > 0;
        // TODO> Ticket - Handling Errors: Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
    }

    /**
     * Deletes comment that matches user email and commentId.
     *
     * @param commentId - commentId string value.
     * @param email     - user email value.
     * @return true if successful deletes the comment.
     */
    public boolean deleteComment(String commentId, String email) {
        Bson query = Filters.eq("_id", new ObjectId(commentId));

        Comment toDelete = commentCollection.find(query).iterator().tryNext();

        if (toDelete == null) {
            return false;
        }

        if (!toDelete.getEmail().equals(email)) {
            return false;
        }


        try {
            DeleteResult result = commentCollection.deleteOne(query);
            return result.getDeletedCount() == 1;
        } catch (MongoWriteException e) {
            log.error("Could not delete result: " + e.getError().getCategory());
            return false;
        }
        // TODO> Ticket Handling Errors - Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
    }

    /**
     * Ticket: User Report - produce a list of users that comment the most in the website. Query the
     * `comments` collection and group the users by number of comments. The list is limited to up most
     * 20 commenter.
     *
     * @return List {@link Critic} objects.
     */
    public List<Critic> mostActiveCommenters() {
        List<Critic> mostActive = new ArrayList<>();
        List<Bson> pipeline = Arrays.asList(group("$email", sum("count", 1)),
                sort(descending("count")), limit(20));

        MongoCollection<Document> collection = db.getCollection(COMMENT_COLLECTION);

        MongoCursor<Document> iterator = collection.aggregate(pipeline).iterator();

        iterator.forEachRemaining(doc -> {
            Critic critic = new Critic();
            critic.setId(doc.getString("_id"));
            critic.setNumComments(doc.getInteger("count"));
            mostActive.add(critic);
        });

        return mostActive;
    }
}
