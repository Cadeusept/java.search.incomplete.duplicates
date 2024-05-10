# JAVA WEBSITE PARSER
## Stack
- Java *21* (Jsoup *1.10.2* for parsing)
- RabbitMQ *3.10.7*
- Elasticsearch *7.17.19*
- Kibana *7.17.19*
- Logstash *7.17.19*
- Filebeat *7.17.19*
## Work algorithm
### 1. "Link Catcher" service
- Parsing website, catching links from base page
- If already in elastic, continue
- New links with news are being sent to RMQ url queue
### 2. "Website Parser" service
- Getting links from url queue
- Check links into hashmap
- If there is no link in hash map, save it and download *.html file
- Earning info from website using css selectors
- Mapping info inside entity named "NewsHeadline"
- Sending entity to RMQ data queue
### 3. "Elasticsearch Consumer" service
- Creating index if not created
- Getting links from data queue
- If already in elastic, continue
- Saving new data to elasticsearch

## Example of work
### 1. "Link Catcher" service and "Website Parser" service
![Alt text](/img/1.png "Screenshot of url queue")
### 2. "Website Parser" service and "Elasticsearch Consumer" service
![Alt text](/img/2.png "Screenshot of data queue")
### 3. Kibana dashboard screenshot
![Alt text](/img/3.png "Screenshot of kibana dashboard")
### 4. Kibana dashboard with filters screenshot
![Alt text](/img/4.png "Screenshot of kibana dashboard with filters")
### 5. Kibana dashboard with logs screenshot
![Alt text](/img/5.png "Screenshot of kibana dashboard with logs")

## Queries and Aggregations
### 1. OR Query
```
POST /news_headlines/_search
{
  "query": {
    "bool": {
      "should": [
        {"match": {"author": "Денис"}},
        {"match": {"body": "Симферополь"}}
      ]
    }
  }
}
```

### AND Query
```
POST /news_headlines/_search
{
  "query": {
    "bool": {
      "must": [
        {"match": {"author": "Денис"}},
        {"match": {"body": "Симферополь"}}
      ]
    }
  }
}
```

### Script Query
```
POST /news_headlines/_search
{
  "query": {
    "script_score": {
      "query": {"match_all": {}},
      "script": {
        "source": "doc['URL'].value.length()"
      }
    }
  }
}
```

### MultiGet Query
```
GET /news_headlines/_mget
{
  "docs": [
    { "_id": "arr7Vo8B5aM0OFyPWyIm" },
    { "_id": "V1q0WY8Bq03dI9uQAZuB" },
    { "_id": "X7r7Vo8B5aM0OFyPSSL_" }
  ]
}
```

### Date Histogram Aggregation
```
POST /news_headlines/_search
{
  "aggs": {
    "articles_per_day": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "day"
      }
    }
  }
}
```

### Date Range Aggregation
```
POST /news_headlines/_search
{
  "aggs": {
    "articles_in_range": {
      "date_range": {
        "field": "date",
        "ranges": [
          {"from": "2022-01-01", "to": "2022-02-01"}
        ]
      }
    }
  }
}
```

### Histogram Aggregation
```
POST /news_headlines/_search
{
  "aggs": {
    "header_length_histogram": {
      "histogram": {
        "script": {
          "source": "doc['header'].value.length()",
          "lang": "painless"
        },
        "interval": 10
      }
    }
  }
}
```

### Terms Aggregation
```
POST /news_headlines/_search
{
  "aggs": {
    "popular_authors": {
      "terms": {
        "field": "author"
      }
    }
  }
}
```

### Filter Aggregation
```
POST /news_headlines/_search
{
  "aggs": {
    "filtered_body": {
      "filter": {
        "term": {"author": "crimea.mk.ru"}
      },
      "aggs": {
        "avg_body_length": {
          "avg": {
            "script": {
              "source": "doc['body'].value.length()",
              "lang": "painless"
            }
          }
        }
      }
    }
  }
}
```

### Logs Aggregation
```
GET /logstash*/_search
{
  "size": 0, 
  "aggs": {
    "streams": {
      "terms": {
        "field": "stream.keyword",
        "size": 10
      }
    }
  }
}
```
