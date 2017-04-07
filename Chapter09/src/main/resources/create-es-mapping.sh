#!/usr/bin/env bash

curl -XDELETE 'http://localhost:9200/gzet'

curl -XPUT 'http://localhost:9200/gzet'

curl -XPUT 'http://localhost:9200/gzet/_mapping/twitter' -d '
{
    "_ttl" : {
           "enabled" : true
    },
    "properties": {
       "body": {
          "type": "string"
       },
       "time": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss"
       },
       "tags": {
          "type": "string",
          "index": "not_analyzed"
       },
       "batch": {
          "type": "integer"
       }
    }
 }
'

curl -XPUT 'http://localhost:9200/gzet/_mapping/gdelt' -d '
{
    "properties": {
       "body": {
          "type": "string"
       },
       "time": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss"
       },
       "tags": {
          "type": "string",
          "index": "not_analyzed"
       },
       "title": {
          "type": "string"
       },
       "url": {
          "type": "string",
          "index": "not_analyzed"
       }
    }
 }
'
