# JAVA WEBSITE PARSER
## Stack
- Java *21* (Jsoup *1.10.2* for parsing)
- RabbitMQ *3.10.7*
- Elasticsearch *7.17.19*
- Kibana *7.17.19*
## Work algorythm
### 1. "Link Catcher" service
- Parsing website, catching links from base page
- Valid links with news are being sent to RMQ url queue
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
- Saving data to elasticsearch

## Example of work
### 1. "Link Catcher" service and "Website Parser" service
![Alt text](/img/1.png "Screenshot of url queue")
### 2. "Website Parser" service and "Elasticsearch Consumer" service
![Alt text](/img/2.png "Screenshot of data queue")
### 3. Kibana dashboard screenshot
![Alt text](/img/3.png "Screenshot of kibana dashboard")
### 4. Kibana dashboard with filters screenshot
![Alt text](/img/4.png "Screenshot of kibana dashboard with filters")