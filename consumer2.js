//Consumer code for taking data from the respective kafka topics and making it available for further manipulation
//The code first constructs anoutput file by executing unix commands through node.js and then converting the file into a list
//After which tehe list is compared with another configuration file containing prefixes of data sources signifying which topics to consider

    var mongoose = require('mongoose');
    var Schema = mongoose.Schema;
    var ttl = require('ol-mongoose-ttl');
    mongoose.connect('mongodb://localhost/sample');

  var TweetSchema = new Schema({
  message: String,
  author: {
    eventId: {
      type: Schema.Types.ObjectId,
      required: true
    },
    eventname: String
  },
  timestamp:  String
},{timestamps:true});

  TweetSchema.index({createdAt: 1},{expireAfterSeconds: 10});

    var Tweet=mongoose.model('Tweet',TweetSchema);


    var MongoClient = require('mongodb').MongoClient;
    var redis = require('redis');
    var client2 = redis.createClient();
    var assert=require('assert');
    var kafka = require('kafka-node');
    var fs=require('fs');
    var Consumer = kafka.Consumer;
    var client = new kafka.Client();
    var consumer = new Consumer(client,[{topic:"Ezest-Ascent-connect-ODApplication",partition:0},{topic:"Ezest-Whiziblem-connect-MileStone",partition:0}
      ,{topic:"Ezest-Ascent-connect-LeaveApplication",partition:0},{topic:"Ezest-Whizible-connect-ProjectRisk",partition:0},{topic:"Ezest-Whiziblem-connect-Customer",partition:0}],
    {
        autoCommit: false,
        fetchMaxBytes: 1024 * 1024
    }
);

var flag=1;
//Exceuting unix command for navigating into the particular directory
var exec=require("child_process").exec;
exec("cd /home/jaideep/confluent-oss-5.0.0-2.11/confluent-5.0.0",function(err,stdout)
{
  if(err)
    throw err;
  else
  {
    flag=0;  
    get_list();
  }
})

//Executing unix command for getting the list of all kafka topics from the zookeeper
function get_list()
{
   exec("/home/jaideep/confluent-oss-5.0.0-2.11/confluent-5.0.0/bin/kafka-topics --zookeeper localhost:2181 -list > /home/jaideep/myapp/output.txt",function(err,stdout)
 {
  if(err)
    throw err;

  console.log("Finished executing second");
  var array_output = fs.readFileSync('/home/jaideep/myapp/output.txt').toString().split("\n");
  console.log(stdout);

})
}

//Converting the file containing the list of all kafka topics into a list

var array_output = fs.readFileSync('output.txt').toString().split("\n");
var len=array_output.length;
var i,l=0;
var array_filter=new Array();
for(i=0;i<len;i++)
{
   if(array_output[i].charAt(0)!='_')
   {
      array_filter[l]=array_output[i];
      l++;
   }
}

//Reading data from a configuration file for finding topics from which data is to be read by kafka
var topic_list=[];
var array_config = fs.readFileSync('topic_config').toString().split(",");
console.log("Array = "+array_config[0]);
var i,k,j=0;

for(k=0;k<array_filter.length;k++)
  {
    for(i=0;i<array_config.length;i++)
    {
      if(array_filter[k].indexOf(array_config[i])>-1 || array_filter[k]==array_config[i])
      {
        topic_list[j]=array_filter[k];
        j++;
      }
    }
  }
//--------------------------------------------------------------------------------------------------------------------------------
var final_topics=[];
var topic_counter=0;
for(i=0;i<topic_list.length;i++)
{
  var obj={};
  obj["topic"]=topic_list[i];
  obj["partition"]=0;
  var obj2=JSON.stringify(obj);
  final_topics[topic_counter]=obj2;
  topic_counter++;
}
//--------------------------------------------------------------------------------------------------------------------------------

//Displaying data read by kafka consumer on NODE console 
consumer.on('message', function (message) {
	if(message.offset!=0)
	{
      Message_formation(message.value);
	}
});

function Message_formation(message)
{
    var jsonParsed = JSON.parse(message);
    // access elements
    var field=jsonParsed.schema.fields;
    for(i=0;i<field.length;i++)
    {
        var name=field[i].name;
        if(name!=undefined)
        {
          if(name.includes("Timestamp"))
          {
            var temp=field[i].field;
            var val=jsonParsed.payload[temp];
            if(val!=null)
            {
              var final_date=new Date(val);
              jsonParsed.payload[temp]=final_date;
            }
          }
        }
    }
    var msg=JSON.stringify(jsonParsed);
    var final_msg=filter_templates(msg);

}

function filter_templates(message)
{
  var name,id;
   MongoClient.connect("mongodb://172.52.90.34:27017/", function(err, db) {
   if(!err) {
        
      var dbo=db.db("TestServerSimpleTwitter");

     var cursor = dbo.collection('events').find();
     {
         cursor.each(function(err, doc) {

               if(doc!=undefined)
               {
                  for(l=0;l<topic_list.length;l++)
                  {
                  if(topic_list[l].includes(doc.parent_name))
                  {
					           if(doc.parent_name!=undefined && doc.parent_id!=undefined)
					           {
					                name=doc.parent_name;
					                id=doc.parent_id;
					           }
				
                    var event_message=doc.event_message;
                    var fields=event_message.split('#');
                    var template_array=[];
                    var counter_template=0;
                    for(i=0;i<fields.length;i++)
                    {
                       if(fields[i]!=" " && fields[i]!=undefined)
                       {
                         template_array[counter_template++]=fields[i];
                       }        
                    }
                    var object={};
                    for(j=0;j<template_array.length;j=j+2)
                    {
                        var jsonParsed = JSON.parse(message);
                        var key = template_array[j];
                        if(typeof(jsonParsed.payload[template_array[j]])!='undefined' && typeof(jsonParsed.payload[template_array[j]])!='null')
                        {
                           object[key]=jsonParsed.payload[template_array[j]];
                        }
                    }
                    var msg=JSON.stringify(object); 
                    
                    if(msg.length>2)
                    {
                      send_to_mongo(msg,name,id);
                    } 
                  }
               }
             }
         });
     }
   }
   if(db!=null)
   db.close();
});
}


function send_to_mongo(msg,name,id)
{
	var tweet=new Tweet({
		'author':{
			'eventId':id,
			'eventname':name
		},
		'message':msg
	});
	console.log("The tweet constructed is ="+tweet);
  
     tweet.save(function(err){
		 if(err)
		 {
      throw err;
			 console.log("There was a problem while solving the tweets");
		 }
		 else
		 {
			 console.log("Tweet saved successfully into the mongodb database");
		 }
	 });
}
consumer.on('error', function (err) {
    console.log('Error:',err);
})

consumer.on('offsetOutOfRange', function (err) {
    console.log('offsetOutOfRange:',err);
})