# LearnCassandraWithGo

1. Run a kafka cluster
    a. Create Topic `BLOG-EVENTS`, using `./kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic`
        Tweak the command according to your environment.
2. Run cassandra cluster
3. Run cqlsh, and create database using the following commands:
    drop keyspace Blog;
    create keyspace Blog with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    create table blog.post(blogID text, authorId text, authorName text, title text, body text, conclusion text, publishedAt text, PRIMARY KEY(blogID));
    create index on blog.post(publishedAt);

4. Look into `constants.go` file, the database, kafka ports are mentioned there. So, set them accordingly.
5. Build binary using: `go build` in the root directory of the project.