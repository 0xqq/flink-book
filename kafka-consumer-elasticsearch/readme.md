ElasticSearch 101

https://app.bonsai.io/clusters/kafka-course-5599481925/console

GET /_cat/health?v                  health of the server
GET /_cat/nodes?v                   node info
GET /_cat/indices?v                 find indices
PUT /twitter                        create an index called "twitter", will get an ACK = true, can't do it twice or you get an error


PUT /twitter/tweets/1/?pretty       this will write a document into that index like the one below. The index is "twitter" and the index type is "tweets". After that is the ID
{
    "name": "John Doe"
}

For the previous command there's no strict structure enforced on the document

DELETE /twitter/tweets/1            deletes the index with id 1 of index type tweets in the twitter index
DELETE /twitter                     deletes the index