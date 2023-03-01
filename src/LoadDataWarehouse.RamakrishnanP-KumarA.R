# --------------------------
# Author: "Pooja Ramakrishnan, Ashwini Shaktivel Kumar"
# Title: "Practicum 2"
# File : LoadDataWarehouse
# Course: "CS5200 Database Management Systems"
# --------------------------

# Loading required libraries:

install.packages("RMySQL")
library(RMySQL)

##Creating a MYSQL database locally:

con = dbConnect(RMySQL::MySQL(),
                dbname='pract',
                host='localhost',
                port=3306,
                user='root',
                password='Ramanathapuram@14')

##Connecting to the sqlite db to load data from Part 1:

# Connecting to SQLite database:
dbfile = "CS5200.Practicum2.sqlite"
consqlite <- dbConnect(RSQLite::SQLite(), paste0(dbfile))

dbListTables(con)

dbSendQuery(consqlite, "drop table if exists AuthorFacts")

#Creating Association Facts table as part of the star schema

##Query to select required rows from the sqlite db for Association facts table

# The Association facts consists of each author, their co-authors and the number of articles they've published
# together.

query <- "SELECT c.Author1, c.coAuthor, count(*) as times_worked_together
FROM (
  SELECT a.AuthorID as Author1, b.AuthorID as coAuthor
  FROM AuthorList a
  INNER join AuthorList b
  ON a.ArticleperAuthorID = b.ArticleperAuthorID AND a.AuthorID != b.AuthorID) c
GROUP BY c.Author1, c.coAuthor"

# Get a dataframe comprising of the query output:
df.af <-dbGetQuery(consqlite,query)

# Drop Association Facts if exists. 
dbGetQuery(con, "drop table if exists AssociationFactss")

# Write Association Fact tables with the states details.
dbWriteTable(con,"AssociationFactss",df.af, append=F, overwrite=T, row.names=0)

dbGetQuery(con, "select * from AssociationFactss")

# Drop Author table if exists. 
dbSendQuery(con, "drop table if exists Author")

# Get required columns from Author table as per requirement. 
###df.Author <- dbGetQuery(consqlite, "select * from Author")
df.Author <- dbGetQuery(consqlite, "select AuthorID, LastName, ForeName from Author")

dbWriteTable(con, "Author" , df.Author , append=F, overwrite=T, row.name=0)

# Drop AuthorList table if exists.
dbSendQuery(con, "drop table if exists AuthorList")

# Get required columns from AuthorList table as per requirement.
###df.AuthorList <- dbGetQuery(consqlite, "select * from AuthorList")
df.AuthorList <- dbGetQuery(consqlite, "select AuthorID, ArticleperAuthorID from AuthorList")

dbWriteTable(con, "AuthorList" , df.AuthorList , append=F, overwrite=T, row.name=0)

#Creating AuthorFacts table as part of the star schema

# Drop Author Fact table if exists.
dbSendQuery(con, "drop table if exists AuthorFacts")

# Query required columns.
query <- "select a.AuthorID, a.LastName, a.ForeName, count(distinct b.ArticleperAuthorID) as ArticlesCount, count(distinct c.coAuthor) as CountCoAuthors 
          from Author a join AuthorList b on a.AuthorID = b.AuthorID
          join AssociationFactss c on a.AuthorID=c.Author1
group by a.AuthorID "

df.AuthorFacts <- dbGetQuery(con, query)

# Write Author Fact table.
dbWriteTable(con, "AuthorFacts" , df.AuthorFacts, overwrite=T,append=F,row.names=0)

dbGetQuery(con, "select * from AuthorFacts")

##Part2.4

#Creating Journal Facts table as part of the star schema

# Drop Article table if exists.
dbSendQuery(con, "drop table if exists Article")

# Get required columns from AuthorList table as per requirement.
###df.Article <- dbGetQuery(consqlite, "select * from Article")
df.Article <- dbGetQuery(consqlite, "select ArticleID, JournalID from Article")

dbWriteTable(con, "Article" , df.Article , append=F, overwrite=T, row.name=0)

# Drop JournalIssue table if exists.
dbSendQuery(con, "drop table if exists JournalIssue")

# Get required columns from AuthorList table as per requirement.
###df.JournalIssue <- dbGetQuery(consqlite, "select * from JournalIssue")
df.JournalIssue <- dbGetQuery(consqlite, "select JournalIssueID, PubDateID from JournalIssue")
dbWriteTable(con, "JournalIssue" , df.JournalIssue , append=F, overwrite=T, row.name=0)

# Drop Journal table if exists.
dbSendQuery(con, "drop table if exists Journal")

# Get required columns from AuthorList table as per requirement.
###df.Journal <- dbGetQuery(consqlite, "select * from Journal")
df.Journal <- dbGetQuery(consqlite, "select JournalID, Title, JournalIssueID from Journal")
dbWriteTable(con, "Journal" , df.Journal , append=F, overwrite=T, row.name=0)

# Drop PubDate table if exists.
dbSendQuery(con, "drop table if exists PubDate")

# Get required columns from AuthorList table as per requirement.
###df.PubDate <- dbGetQuery(consqlite, "select * from PubDate")
df.PubDate <- dbGetQuery(consqlite, "select PubDateID, tDate from PubDate")
dbWriteTable(con, "PubDate" , df.PubDate , append=F, overwrite=T, row.name=0)

dbGetQuery(con, "select * from JournalIssue")

dbGetQuery(con, "select * from PubDate")

#Creating Journal Fact

dbSendQuery(con, "drop table if exists JournalFact")

df.JournaFact <- dbGetQuery(con, "select b.ArticleID, a.JournalID, a.Title, d.tDate
          from Journal a join Article b on a.JournalID = b.JournalID
          join JournalIssue c on a.JournalIssueID = c.JournalIssueID
          join PubDate d on c.PubDateID = d.PubDateID")

dbWriteTable(con, "JournalFact" , df.JournaFact , append=F, overwrite=T, row.name=0)
dbGetQuery(con, "select * from JournalFact")

dbGetQuery(con, "select *
          from Journal a join Article b on a.JournalID = b.JournalID
          join JournalIssue c on a.JournalIssueID = c.JournalIssueID
          join PubDate d on c.PubDateID = d.PubDateID")

dbGetQuery(con, "select *
          from Journal a join Article b on a.JournalID = b.JournalID
          join JournalIssue c on a.JournalIssueID = c.JournalIssueID")

#Example : What the are number of articles published in every journal in 1975?

dbGetQuery(con, "select year(tDate) as year, count(ArticleID) as noOfArticles from JournalFact having year = 1975")

#Part 3

dbGetQuery (con, "select Title, QUARTER(tDate) as quarter, year(tDate) as year, count(ArticleID) from JournalFact group by Title,year,quarter order by year, quarter limit 20 ")

dbDisconnect(consqlite)