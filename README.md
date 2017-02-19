# lastfm

## Question:
Go to the following URL, and download the dataset on sampled Last.fm usage: http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html
Provide a series of Spark Jobs to answers the questions below. For each question, describe alternative approaches you would have used instead of Spark, and provide the source that you wrote to solve the problem.

## Part A
Create a list of user IDs, along with the number of distinct songs each user has played.

## Part B
Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of times each was played.

## Part C
Say we define a user’s “session” of Last.fm usage to be comprised of one or more songs played by that user, where each song is started within 20 minutes of the previous song’s start time. Create a list of the top 10 longest sessions, with the following information about each session: userid, timestamp of first and last songs in the session, and the list of songs played in the session (in order of play).

## Compile the project
    > sbt package

## Usage
Please input exact two application arguments `Option` and `FilePath`,
whereas Option should be A, B or C, and FilePath should be the path to the input file.
For example:

    > SPARK_DIR/bin/spark-submit \
    >   --class "LastFmApp" \
    >   --master local[4] \
    >   target/scala-2.11/lastfm_2.11-1.0.jar \
    >   A \
    >   /path-to-file/userid-timestamp-artid-artname-traid-traname.tsv