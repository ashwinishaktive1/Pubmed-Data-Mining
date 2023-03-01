# --------------------------
# Author: "Pooja Ramakrishnan, Ashwini Shaktivel Kumar"
# Title: "Practicum 2"
# File : LoadXML2DB
# Course: "CS5200 Database Management Systems"
# --------------------------

# Loading required libraries:
library(RSQLite)
library(XML)
library(parallel)
library(plyr)
library(lubridate)

# Connecting to SQLite database:
dbfile = "CS5200.Practicum2.sqlite"
dbcon <- dbConnect(RSQLite::SQLite(), paste0(dbfile))

# Dropping pre-existing tables:

sql <- paste0(
  "DROP TABLE IF EXISTS Author"
)

dbExecute(dbcon, sql)

sql <- paste0(
  "DROP TABLE IF EXISTS AuthorList"
)

dbExecute(dbcon, sql)

sql <- paste0(
  "DROP TABLE IF EXISTS PubDate"
)

dbExecute(dbcon, sql)

sql <- paste0(
  "DROP TABLE IF EXISTS JournalIssue"
)

dbExecute(dbcon, sql)

sql <- paste0(
  "DROP TABLE IF EXISTS Journal"
)

dbExecute(dbcon, sql)

sql <- paste0(
  "DROP TABLE IF EXISTS Article"
)

dbExecute(dbcon, sql)

sql <- paste0(
  "DROP TABLE IF EXISTS PubmedArticle"
)

dbExecute(dbcon, sql)

# Table creation:

# Author:

sql <- paste0(
  "CREATE TABLE Author (",
  
  "AuthorID NUMERIC NOT NULL,",
  "LastName TEXT,",
  "ForeName TEXT,",
  "Initials TEXT,",
  "Suffix TEXT,",
  
  "PRIMARY KEY (AuthorID)",
  ")"
)

dbExecute(dbcon, sql)

# AuthorList:

sql <- paste0(
  "CREATE TABLE AuthorList (",
  
  "AuthorListID INTEGER NOT NULL,",
  "AuthorID INTEGER NOT NULL,",
  "ArticleperAuthorID INTEGER NOT NULL,",
  
  "PRIMARY KEY (AuthorListID),",
  "FOREIGN KEY (AuthorID) REFERENCES Author(AuthorID)",
  "FOREIGN KEY (ArticleperAuthorID) REFERENCES Article(ArticleperAuthorID)",
  
  ")"
)

dbExecute(dbcon, sql)

# PubDate:

sql <- paste0(
  "CREATE TABLE PubDate (",
  "Year INTEGER,",
  "Month TEXT,",
  "Day INTEGER,",
  "MedlineDate TEXT,",
  "PubDateID INTEGER,",
  "tDate DATETIME,",
  "PRIMARY KEY (PubDateID)",
  ")"
)

dbExecute(dbcon, sql)

# JournalIssue:

sql <- paste0(
  "CREATE TABLE JournalIssue (",
  
  "JournalIssueID NUMERIC NOT NULL,",
  "Volume INTEGER,",
  "Issue INTEGER,",
  "PubDateID TEXT NOT NULL,",
  "CitedMedium TEXT NOT NULL,",
  
  "PRIMARY KEY (JournalIssueID),",
  "FOREIGN KEY (PubDateID) REFERENCES PubDate(PubDateID)",
  ")"
)

dbExecute(dbcon, sql)

# Journal:

sql <- paste0(
  "CREATE TABLE Journal (",
  
  "JournalIssueID NUMERIC NOT NULL,",
  "JournalID NUMERIC NOT NULL,",
  
  "Title TEXT,",
  "ISOAbbreviation TEXT,",
  "ISSN TEXT,",
  "ISSNType TEXT,",
  
  "PRIMARY KEY (JournalID),",
  "FOREIGN KEY (JournalIssueID) REFERENCES JournalIssue(JournalIssueID)",
  ")"
)

dbExecute(dbcon, sql)

# Article:

sql <- paste0(
  "CREATE TABLE Article (",
  
  "ArticleID NUMERIC NOT NULL,",
  "ArticleperAuthorID NUMERIC NOT NULL,",
  "AuthorListID NUMERIC NOT NULL,",
  "JournalID NUMERIC NOT NULL,",
  
  "Language TEXT,",
  "ArticleTitle TEXT,",
  
  "PRIMARY KEY (ArticleperAuthorID),",
  "FOREIGN KEY (JournalID) REFERENCES Journal(JournalID)",
  "FOREIGN KEY (AuthorListID) REFERENCES AuthorList(AuthorListID)",
  ")"
)

dbExecute(dbcon, sql)

# Loading XML document:

xmlfn <- "pubmed-tfm-xml-subset.xml"

# Reading the XML file and parse into DOM
xmlDOM <- xmlParse(file = xmlfn)

# get the root node of the DOM tree
r <- xmlRoot(xmlDOM)

# No. of child node layers under the root node. 
numPO <- xmlSize(r)

# Pubmed Articles Set dataframe
PMA.df <- data.frame (PMID = vector (mode = "integer", 
                                     length = numPO),
                      ArticleID = vector (mode = "integer", 
                                          length = numPO),
                      stringsAsFactors = F)
# Pubmed Articles dataframe
pm.df <- data.frame(PMID = integer(),
                    ArticleID = integer(),
                    stringsAsFactors = F)
# Author dataframe
Author.df <- data.frame(AuthorID = integer(),
                        ValidYN = character(),
                        LastName = character(),
                        ForeName = character(),
                        Initials = character(),
                        Suffix = character(),
                        stringsAsFactors = F)

# AuthorList dataframe
AuthorList.df <- data.frame(AuthorListID = integer(),
                            ArticleID = integer(),
                            CompleteYN = character(),
                            AuthorID = integer(),
                            stringsAsFactors = F)
# PubDate dataframe
PubDate.df <- data.frame(PubDateID = integer(),
                         Year = integer(),
                         Month = character(),
                         Day = integer(),
                         MedlineDate = character(),
                         stringsAsFactors = F)

# JournalIssue dataframe
JournalIssue.df <- data.frame(JournalIssueID = integer(),
                              CitedMedium = character(),
                              Volume = integer(),
                              issue = integer(),
                              PubDateID = character(),
                              stringsAsFactors = F)
# Article dataframe
Article.df <- data.frame (ArticleID = integer(),
                          ArticleperAuthorID = integer(),
                          JournalID = integer(),
                          Language = character(),
                          ArticleTitle = character(),
                          AuthorListID = integer(),
                          stringsAsFactors = F)

#Journal dataframe
Journal.df <- data.frame (JournalID = integer(),
                          JournalIssueID = integer(),
                          ISSN = character(),
                          ISSNType = character(),
                          Title = character(),
                          ISOAbbreviation = character(),
                          stringsAsFactors = F)

# Logic motivated from : http://artificium.us/lessons/06.r/l-6-328-xml-to-reldb-sqlite/l-6-328.html

# Function to check if the parsed row already exists in the look-up or dictionary child tables. 

rowExists <- function (aRow, aDF)
{
  # check if that row is already in the data frame
  n <- nrow(aDF)
  c <- ncol(aDF)
  
  if (n == 0)
  {
    # data frame is empty, so can't exist
    return(0)
  }
  
  for (a in 1:n)
  {
    # check if all columns match for a row; ignore the ID column
    if (all(aDF[a,] == aRow[1,]))
    {
      # found a match; return it's ID
      return(a)
    }
  }
  
  # none matched
  return(0)
}

# Parsing pubDate information.

parsePubDate <- function(PMID){
  # For each PMID retrieving the corresponding publication date details. 
  year <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/JournalIssue/PubDate/Year"), xmlValue)
  month <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/JournalIssue/PubDate/Month"), xmlValue)
  day <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/JournalIssue/PubDate/Day"), xmlValue)
  MedlineDate <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/JournalIssue/PubDate/MedlineDate"), xmlValue)
  
  # Checking if the elements are absent.
  if(length(year) == 0){
    year <- ''
  }
  if(length(month) == 0){
    month <- ''
  }
  if(length(day) == 0){
    day <- ''
  }
  if(length(MedlineDate) == 0){
    MedlineDate <- ''
  }
  
  # Appending the elements to a new dataframe row. 
  y <- data.frame(Year = year, Month = month, Day = day, MedlineDate = MedlineDate, stringsAsFactors = F)
  
  return(y)
}

pmidlist <- seq.int(1,numPO)
# Binding the returned new dataframe row to the parent PubDate dataframe. 
PubDate.df <- as.data.frame(do.call("rbind", mclapply(pmidlist, parsePubDate)), stringsAsFactors = F)

# Finding unique rows, scrapping duplicate ones. 
PubDate.df <- unique(PubDate.df)

# Assigning pubDateID to each row as the Primary Key. 
PubDate.df$PubDateID <- 1:nrow(PubDate.df)

# We have followed a similar mechanism as above for all the remaining table retrievals. This is a recursive operation
# where we each time compare the retrieved details of the child node with the existing dictionary table (for example here
# we compare the retrieved PubDate information for a PMID with the rows in PubDate.df, if the row exists then
# we get the corresponding PubDateID as a reference). The early creation of PubDate.df ensures that all the possible
# values of publication date are pre-emptively retrieved. Each time we build a component table, we find it's
# unique rows and assign an unique identifier (primary key) allowing us to refer to the row from other tables using the 
# identifier as a foreign key. 

# Parsing Journal Issue details. 

parseJournalIssue <- function(PMID){
  citedMedium <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/JournalIssue"), xmlAttrs)
  volume <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/JournalIssue/Volume"), xmlValue)
  issue <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/JournalIssue/Issue"), xmlValue)
  pubdatenew <- parsePubDate(PMID)
  
  if(length(citedMedium) == 0){
    citedMedium <- ''
  }
  if(length(volume) == 0){
    volume <- ''
  }
  if(length(issue) == 0){
    issue <- ''
  }
  
  pubdateIDrow <- rowExists(pubdatenew, PubDate.df[1:4])
  
  y <- data.frame(CitedMedium = citedMedium, Volume = volume, Issue = issue, PubDateID = pubdateIDrow, stringsAsFactors = F)
  
  return(y)
}

pmidlist <- seq.int(1,numPO) 
JournalIssue.df <- as.data.frame(do.call("rbind", mclapply(pmidlist, parseJournalIssue)), stringsAsFactors = F)

JournalIssue.df <- unique(JournalIssue.df)
JournalIssue.df$JournalIssueID <- 1:nrow(JournalIssue.df)

# Parsing Journal details. 

parseJournal <- function(PMID){
  ISSN <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/ISSN"), xmlValue)
  ISSNType <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/ISSN"), xmlAttrs)
  Title <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/Title"), xmlValue)
  ISOAbbreviation <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Journal/ISOAbbreviation"), xmlValue)
  JournalIssue <- parseJournalIssue(PMID)
  
  if(length(ISSN) == 0){
    ISSN <- ''
  }
  if(length(ISSNType) == 0){
    ISSNType <- ''
  }
  if(length(Title) == 0){
    Title <- ''
  }
  if(length(ISOAbbreviation) == 0){
    ISOAbbreviation <- ''
  }
  
  JournalIssueID <- rowExists(JournalIssue, JournalIssue.df[1:4])
  
  y <- data.frame(ISSN=ISSN, ISSNType=ISSNType, Title=Title, ISOAbbreviation=ISOAbbreviation , JournalIssueID=JournalIssueID , stringsAsFactors = F)
  
  return(y)
}

pmidlist <- seq.int(1,numPO) 
Journal.df <- as.data.frame(do.call("rbind", mclapply(pmidlist, parseJournal)), stringsAsFactors = F)

Journal.df <- unique(Journal.df)
Journal.df$JournalID <- 1:nrow(Journal.df)

# Parsing only the brief details of each article required to generate unique identifier ArticleID. 

parseArticle <- function(PMID){
  journal <- parseJournal(PMID)
  language <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/Language"), xmlValue)
  articleTitle <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/ArticleTitle"), xmlValue)
  
  if(length(language) == 0){
    language <- ''
  }
  if(length(articleTitle) == 0){
    articleTitle <- ''
  }
  
  journalIDrow <- rowExists(journal, Journal.df[1:5])
  
  y <- data.frame(JournalID = journalIDrow, Language = language, ArticleTitle = articleTitle, ArticleID = PMID, stringsAsFactors = F)
  
  return(y)
}

pmidlist <- seq.int(1,numPO) 
Article_temp.df <- as.data.frame(do.call("rbind", mclapply(pmidlist, parseArticle)), stringsAsFactors = F)

Article_temp.df <- unique(Article_temp.df)
Article_temp.df$ArticleID <- 1:nrow(Article_temp.df)
Article_temp.df$ArticleperAuthorID <- 1:nrow(Article_temp.df)

# Parsing Author details onto the Author dataframe. 

parseAuthor <- function(PMID){
  
  noOfAuthors <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/AuthorList"), xmlSize)
  
  if(length(noOfAuthors)>0){
    df <- data.frame(LastName = character(), ForeName = character(), Initials = character(), Suffix = character(), stringsAsFactors = F)
    for (i in 1:noOfAuthors){
      lastName <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/AuthorList/Author[", i, "]/LastName"), xmlValue)
      foreName <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/AuthorList/Author[", i, "]/ForeName"), xmlValue)
      initials <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/AuthorList/Author[", i, "]/Initials"), xmlValue)
      suffix <- xpathSApply(xmlDOM, paste("//PubmedArticle[@PMID=", PMID, "]/Article/AuthorList/Author[", i, "]/Suffix"), xmlValue)
      
      if(length(lastName) == 0){
        lastName <- ''
      }
      if(length(foreName) == 0){
        foreName <- ''
      }
      if(length(initials) == 0){
        initials <- ''
      }
      if(length(suffix) == 0){
        suffix <- ''
      }
      
      y <- data.frame(LastName = lastName, ForeName = foreName, Initials = initials, Suffix = suffix, stringsAsFactors = F)
      df[i, ] <- y
    }
    return(df)
  }
}

pmidlist <- seq.int(1,numPO) 
Author.df <- as.data.frame(do.call("rbind", mclapply(pmidlist, parseAuthor)), stringsAsFactors = F)

Author.df <- unique(Author.df)
Author.df$AuthorID <- 1:nrow(Author.df)

# Parsing Authorlist junction table resolving many to many relationship between articles and authors. 

parseAuthorList <- function(PMID){
  df <- data.frame(ArticleperAuthorID = integer(), AuthorID = integer(), stringsAsFactors = F)
  authors <- parseAuthor(PMID)
  article <- parseArticle(PMID)
  
  articleIDrow <- rowExists(article, Article_temp.df[1:4])
  
  if(!is.null(nrow(authors))){
    for(i in 1:nrow(authors)){
      authorIDrow <- rowExists(authors[i,], Author.df[1:4])
      y <- data.frame(ArticleID = articleIDrow, AuthorID = authorIDrow, stringsAsFactors = F)
      df[i, ] <- y
    }
    return(df)
  }
}

pmidlist <- seq.int(1,numPO) 
AuthorList.df <- as.data.frame(do.call("rbind", mclapply(pmidlist, parseAuthorList)), stringsAsFactors = F)

AuthorList.df <- unique(AuthorList.df)
AuthorList.df$AuthorListID <- 1:nrow(AuthorList.df)

# Parsing final Article parent table with all the required information. 

parsefinalArticle <- function(PMID){
  df <- data.frame(JournalID = integer(), Language = character(), ArticleTitle = character(), ArticleID = integer(), AuthorListID = integer(), stringsAsFactors = F)
  
  authorList <- parseAuthorList(PMID)
  article <- parseArticle(PMID)
  
  articleIDrow <- rowExists(article, Article_temp.df[1:4])
  
  if(!is.null(nrow(authorList))){
    for(i in 1:nrow(authorList)){
      authorIDrow <- rowExists(authorList[i,], AuthorList.df[1:2])
      y <- data.frame(JournalID = article$JournalID, Language = article$Language, ArticleTitle = article$ArticleTitle, ArticleID = article$ArticleID, AuthorListID = authorIDrow, stringsAsFactors = F)
      df[i, ] <- y
    }
    return(df)
  }
}

pmidlist <- seq.int(1,numPO) 
Article.df <- as.data.frame(do.call("rbind", mclapply(pmidlist, parsefinalArticle)), stringsAsFactors = F)

Article.df <- unique(Article.df)
Article.df$ArticleperAuthorID <- 1:nrow(Article.df)

# Editing date format of pubdate.
# Missing month and day values are by default assigned as 1. We maintain the date column as text, allowing us 
# to push it in the same format onto sql databases. 

# Reference: http://artificium.us/lessons/06.r/l-6-306-dates-in-r-and-sql/l-6-306.html#Saving_Date_Data_to_SQLite

PubDate.df$MedlineDate <- substr(PubDate.df$MedlineDate,1,nchar(PubDate.df$MedlineDate)-4)

for (i in 1:nrow(PubDate.df)){
  if(PubDate.df$MedlineDate[i] != ""){
    PubDate.df$Year[i] <- substr(PubDate.df$MedlineDate[i],1,nchar(PubDate.df$MedlineDate[i])-4)
    PubDate.df$Month[i] <- substr(PubDate.df$MedlineDate[i],6,nchar(PubDate.df$MedlineDate[i])+1)}
  PubDate.df$Month[i] <- match(PubDate.df$Month[i], month.abb)
  
  if(is.na(PubDate.df$Month[i])){
    PubDate.df$Month[i] = "1"
  }
  
  if(PubDate.df$Day[i] == ""){
    PubDate.df$Day[i] = "1"
  }
  PubDate.df$tDate[i] <- paste(PubDate.df$Year[i],PubDate.df$Month[i], PubDate.df$Day[i], sep="-")
}

# Loading the dataframes onto SQLite database.

dbGetQuery(dbcon, "pragma table_info('PubDate')")

# Write Table
dbWriteTable(dbcon,"PubDate",PubDate.df, append=T)

# View Table
dbGetQuery(dbcon, "select * from PubDate limit 5")

dbWriteTable(dbcon,"JournalIssue",JournalIssue.df, append=TRUE)

dbGetQuery(dbcon, "select * from JournalIssue limit 5")

dbWriteTable(dbcon,"Journal",Journal.df, append=TRUE)

dbGetQuery(dbcon, "select * from Journal limit 5")

dbWriteTable(dbcon,"Author",Author.df, append=TRUE)

dbGetQuery(dbcon, "select * from Author limit 5")

dbWriteTable(dbcon,"AuthorList",AuthorList.df, append=TRUE)

dbGetQuery(dbcon, "select * from AuthorList limit 5")

dbWriteTable(dbcon,"Article",Article.df, append=TRUE)

dbGetQuery(dbcon, "select * from Article limit 5")

# Disconnect from database.
dbDisconnect(dbcon)
