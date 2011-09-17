localsettings.py needs to be created containing
api_key = "your last fm key"


MongoDB is used to track users and hold cached, compiled artist information.
The user DB is layed out like:
{ "_id" : ObjectId("4d69676b92f31d06db0aaca7"), 
  "nick" : [ "spikyteddybear", "h", "psym" ], 
  "account" : "visidex", 
  "host" : "whatnetf69_3tf_gvrca9_ip", 
  "network" : "what" }


The artist DB is layed out like:
{ "_id" : ObjectId("4e52e3cc456244f173c337ad"), 
  "bio" : "bio is broke", 
  "stats" : { "listeners" : 6062, "playcount" : 72119 }, 
  "name" : "Karna", 
  "tags" : [
        {
                "count" : 100,
                "name" : "dark ambient"
        },
        {
                "count" : 62,
                "name" : "black metal"
        },
        { ... } ],
  "url" : "http://www.last.fm/music/Karna", 
  "expiration_date" : "Sat Sep 17 2011 02:43:00 GMT+0100 (BST)", 
  "creation_date" : "Tue Sep 13 2011 02:43:00 GMT+0100 (BST)", 
  "mbid" : "f52ac5ee-2f07-40cc-adac-fb2b456be302", 
  "key" : "karna" }


Varnish is used to cache last.fm calls.  Script uses localhost:6081 to connect to cache, or replace in api_url_base with 'ws.audioscobbler.com' to cache nothing.


Varnish VCL:
backend default {
#  ws.audioscrobbler.com may resolve into muliple IPs which varnish doesnt like
#    .host = "ws.audioscrobbler.com";
     .host = "195.24.232.205";
     .port = "80";
}

sub vcl_recv {
     set req.http.host = "ws.audioscrobbler.com";
     return (lookup);
  }