# Twitter Neo4J CUDD Team
Utilizing the twitter API to collect various tweets related to the novel COVID-19 virus and storing those data in Neo4j Graph Database.

## Getting Started
These instructions will get you a copy of twitter data either on local or remote Neo4j database.

### Prerequisites

Before running the application, first you need to have at Python 2.7 and above. It can be found  [here](https://www.python.org/downloads/).
Ater installing Python, you need to install pip packages that our script is dependent on.
With pip you can install tweepy, which is is a library that we used to access Twitter API
```shell
$ pip install tweepy
```
With pip you can install neo4j driver which is a library we used to connect to Neo4j database
```shell
$ pip install neo4j
```

### Additional requirments
Please note in order to access Twitter API you need to have a Twitter developer account where you can sign up in order to obtain 'consumer key' and 'access token'.<br />
Also you need to have either local or remote Neo4j graph database in order to save the resulting data.

## Authors
Khaled Badran 40069733<br />
Emile Tabbakh 40026110<br />
Robert Beaudenon 40022364<br />
