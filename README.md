# MongoDesigns
###  Adjust the constants Const.java to appropriate parameters.

### Generate data in Postgres
1. Run createTables.sql to create the tables.
2. Once tables are created run DataGenerator.java (Have to change Postgres credentials)

### Copy data to MongoDB

3. Run postgres2mongo.java
4. Run readauthids.java

### Generate designs in MongoDB

5. Run putDesigns.java 

>Note: This runs two mongo instances on two different ports (27017 for main design and 27018 for new design). 

### After the running finishes you have to create the indexes manually for each collection ( todo in next improvement)

6. Run experiments with designRuntimes.java

# Careful with the clear.sh you may have to adjust it as it clears all the cache from the file system
