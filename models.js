
var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var ttl = require('mongoose-ttl');


//Define schemas
var UserSchema = new Schema({
	username: String,
	password: String,
	email: String,
	name: String,
	registered: { type: Date, default: Date.now },
	following: [{
		eventId: {
			type: Schema.Types.ObjectId,
			required: true
		},
		eventname: String
	}],
	_follow: Boolean
});


UserSchema.virtual('follow')
	.set(function(follow) {
		this._follow = follow;
	})
	.get(function() {
		return this._follow;
	});

	
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
});
TweetSchema.plugin(ttl, { ttl: 3600000 });

var EventSchema = new Schema({
	eventname: String,
	restricted:String,
    parent_id: {
			type: Schema.Types.ObjectId,
			required: true
		},
    parent_name:String,
    event_message: String,
	event_fields: String,
	topic_definition: [{
		 key: String,
		 value: String
	    }],
  topic_description: {
 		type: Array, default: []
  },
	createdby:String,
    createdon:Date,
   follow : Boolean
});

var TemplateSchema=new Schema({
	eventname: String,
	templates: [{
		 template_name: String,
		template_message: String,
        template_fields: String
	}]
});

var UserTweetFlagSchema=new Schema({
	    tweet_id:  {
			       type: Schema.Types.ObjectId,
			        required: true
		           },
       	username:String,
		tweet_flag:String,
		createdby:String,
        createdon:Date

});

var RecentSearchSchema=new Schema({
	emp_id: String,
    description: { type: Array, default: [] },
	date_time: { type: Date, default: Date.now },
	sequence: String
});
var CommentSchema=new Schema({
	commentVal: String
});
//Create models out of schemas
var User = mongoose.model('User', UserSchema);
var Tweet = mongoose.model('Tweet', TweetSchema);
var Event = mongoose.model('Event', EventSchema);
var Template = mongoose.model('Template', TemplateSchema);
var UserTweetFlag = mongoose.model('UserTweetFlag', UserTweetFlagSchema);
var RecentSearch = mongoose.model('RecentSearch', RecentSearchSchema);
var Comment = mongoose.model('Comment', CommentSchema);

//Handler
/*
UserSchema.pre('save', function(next, done) {
	//TODO only update if name or username was changed

	Tweet.update(
	{'author.userId': this.id}, {
	author: {userId: this.id, username: this.username, name: this.name}},
	{multi: true}, function(err, numberAffected, raw) {
	console.log('[Tweet] The number of updated documents was %d', numberAffected);	next(err);
	});
});
	*/
