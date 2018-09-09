var auth = require(__dirname + '/auth');
var mongoose = require('mongoose');
var bcrypt = require('bcrypt-nodejs');
//var popup = require('popups');
//var url = require('url');
//var stringify = require('json-stringify');
// var idList = [];

exports.init = function(app, Config) {

    // Two test requests to make sure the api works
    app.get('/api/ping', function(req, res) {
        res.json({
            'pong': true
        });
    });
    app.post('/api/ping', function(req, res) {
        res.json({
            'pong': req.body
        });
    });

    // User API 


    if (Config.registration) {
        app.post('/api/users', function(req, res) {
            var User = mongoose.model('User');
            //TODO validation
            var user = new User(req.body);
            User.count().or([{
                'username': user.username
            }, {
                'email': user.email
            }]).exec(function(err, count) {
                if (err) {
                    console.log('There was an error while counting existing users: ' + err);
                    return res.json({
                        'error': 'Couldn\'t hash the password'
                    }, 500);
                }
                if (count >= 1) {
                    return res.json({
                        'error': 'A user with this username or email exists already'
                    }, 500);
                }

                bcrypt.hash(user.password, null, null, function(err, hash) {
                    if (err) {
                        console.log('There was an error while hashing the password: ' + err);
                        return res.json({
                            'error': 'Couldn\'t hash the password'
                        }, 500);
                    }
                    user.password = hash;

                    user.save(function(err) {
                        if (err) {
                            console.log('There was an error while storing the user to the database: ' + err);
                            return res.json({
                                'error': 'Couldn\'t store the user'
                            }, 500);
                        } else {
                            console.log('A new user was created with the id ' + user.id);
                            return res.json({
                                'successful': true
                            });
                        }
                    });
                });
            });
        });
    }
   
    app.post('/api/template',auth.authenticate, function(req, res) {

        var Event = mongoose.model('Event');
        var User = mongoose.model('User');
        var event = new Event(req.body);

        User.findOne({
            'username': req.query.username
        }, function(err, user) {
            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }
            // user.following.push(req.body.eventname);
            // idList.push(req.body.eventname);

            return res.json({
                'user': user
            });
        });


        Event.count().or({
            'eventname': req.body.eventname
        }).exec(function(err, count) {
            if (err) {
                console.log('There was an error while counting existing event: ' + err);
                return res.json({
                    'error': 'Couldn\'t hash the password'
                }, 500);
            }

            if (count >= 1) {
                return res.json({
                    'error': 'Topic already exist! '
                }, 500);
            }


            console.log("current Logged in user::" + req.session.username);
            console.log("Parent Id::" + req.body.parent_id);

            // var datetime = new Date();
            console.log("created date::" + req.body.createdon);
            var eventData = new Event({
                'eventname': req.body.eventname,
                'restricted': req.body.restricted,
                'parent_id': req.body.parent_id,
                'parent_name': req.body.parent_name,
                'event_message': req.body.event_message,
                'event_fields': req.body.event_fields,
                'topic_definition': req.body.topic_definition,
                'topic_description': req.body.topic_description,
                //'':req.body.review_description,
                //'':req.body.review_flag,
                'createdby': req.session.username,
                'createdon': req.body.createdon,
                'follow': req.body.follow

            });
            eventData.save(function(err) {
                if (err) {
                    console.log('There was an error while storing the event to the database: ' + err);
                    return res.json({
                        'error': 'Couldn\'t store the event'
                    }, 500);
                } else {
                    console.log('A new event was created with the id ' + event.id);
                    return res.json({
                        'successful': true
                    });
                }
            });
        });
    });
    app.post('/api/usertweets/saveflag', function(req, res) {

        console.log("save Flag::" + req.body._id);

        var UserTweetFlag = mongoose.model('UserTweetFlag');

        //-------------------
        //checking already tweet flag entry is available.
        UserTweetFlag.findOne({
            'tweet_id': req.body._id,
            'username': req.session.username,
            'tweet_flag': 'N'
        }, function(err, userTweetFlag) {
            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load tweet flag data'
                }, 500);
            }

			console.log("test::"+JSON.stringify(userTweetFlag));
            if (!userTweetFlag) {
                //new entry
                console.log("new entzry");
                console.log('No tweet flag with tweet id found');

                var currentDate = new Date();
                var userTweetFlag = new UserTweetFlag({
                    'tweet_id': req.body._id,
                    'username': req.session.username,
                    'tweet_flag': 'Y',
                    'createdby': req.session.username,
                    'createdon': currentDate
                });

                userTweetFlag.save(function(err) {
                    if (err) {
                        console.log('There was an error while storing the flag tweet to the database: ' + err);
                        return res.json({
                            'error': 'Couldn\'t store the event'
                        }, 500);
                    } else {
                        return res.json({
                            'successful': true
                        });
                    }
                });
            } else {
                //update old entry
                console.log("update old entry");
                UserTweetFlag.update(
				   {
                        'tweet_id': req.body._id,
                        'username': req.session.username
                    },

				    {
                        '$set': {
                            'tweet_flag': 'Y'
                        }
                    },
                    function(err, numberAffected, raw) {
                        if (err) {
                            console.log('Couldn\'t update userTweetFlag');
                            return res.json({
                                'error': 'Couldn\'t update userTweetFlag'
                            }, 500);
                        }

                        return res.json({
                            'successful': true
                        });
                    });
            }

        });

        //--------------------------
        /* userTweetFlag.save(function(err) {
            if (err) {
                console.log('There was an error while storing the flag tweet to the database: ' + err);
                return res.json({
                    'error': 'Couldn\'t store the event'
                }, 500);
            } else {
                console.log('A new flag tweet  \was created with the id ' + event.id);
                return res.json({
                    'successful': true
                });
            }
        });*/
    });

    app.post('/api/usertweets/deleteflag', function(req, res) {

        console.log("delete Flag tweet id::::" + req.body._id);

        var UserTweetFlag = mongoose.model('UserTweetFlag');

        UserTweetFlag.update(
		    {
                'tweet_id': req.body._id,
                'username': req.session.username
            },

			{
                '$set': {
                    'tweet_flag': 'N'
                }
            },
		 function(err, numberAffected, raw) {
                if (err) {
                    console.log('Couldn\'t update userTweetFlag');
                    return res.json({
                        'error': 'Couldn\'t update userTweetFlag'
                    }, 500);
                }

                return res.json({
                    'successful': true
                });
            });

    });
    app.get('/api/users/:username', auth.authenticate, function(req, res) {
        var User = mongoose.model('User');
        User.findOne({
            'username': req.params.username
        }, function(err, user) {
            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }
            if (!user) {
                console.log('No user with this username was not found1');
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 404);
            }

            res.json({
                'user': user
            });
        });
    });

    // app.get('/api/users/:username', auth.authenticate, function(req, res) {
    //     var User = mongoose.model('User');
    //     User.findOne({
    //         'username': req.params.username
    //     }, function(err, user) {
    //         if (err) {
    //             console.log('There was an error while loading data from the database: ', err);
    //             return res.json({
    //                 'error': 'Couldn\'t load user'
    //             }, 500);
    //         }
    //         if (!user) {
    //             console.log('No user with this username was not found1');
    //             return res.json({
    //                 'error': 'Couldn\'t load user'
    //             }, 404);
    //         }

    //         res.json({
    //             'user': user
    //         });
    //     });
    // });



    // Tweet API
    app.get('/api/event/:eventname/tweets/:skip/:limit', auth.authenticate, function(req, res) {

        var Tweet = mongoose.model('Tweet');
        var Events = mongoose.model("Event");
        console.log(Events+"---events");
        //TODO add filter functions
        /*	Tweet.find({'author.username': req.params.username}, null, {'sort': {'timestamp': -1}},{'skip':req.params.skip},{'limit':req.params.limit}, function(err, tweets) {
        		if(err) {
        			console.log('There was an error while loading data from the database: ', err);
        			return res.json({'error': 'Couldn\'t load tweets for the user'}, 500);
        		}
        		res.json({'tweets': tweets});
        	});*/
        //.find({"author.username":"EmployeeJoining"}).sort({"_id":1}).limit(5);

        Tweet.find({
            "author.eventname": req.params.eventname
        }).sort({
            'timestamp':
                -1
        }).skip(req.params.skip).limit(req.params.limit).exec(function(err, tweets) {
            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load tweets for the user dashboard'
                }, 500);
            }

            res.json({
                'tweets': tweets
            });
        });

    });
    // var promise = $http.get('/api/users/:username/:searchCriteriaData/:dashboard/:skip/:limit',auth.authenticate);
    // promise.then(
    //     function(payload) {
    //       $scope.movieContent = payload.data;
    //     });
    app.get('/api/users/:username/:searchCriteriaData/:displaytweet/:dashboard/:skip/:limit', auth.authenticate, function(req, res) {

        var idList = [];
        var searchCriteriaData = req.params.searchCriteriaData;

		console.log("username::"+username);

		console.log("displaytweet in API call::"+req.params.displaytweet);


        var conditionArray = [];
        if (searchCriteriaData == 'undefined') {} else {
            searchCriteriaData = JSON.parse(searchCriteriaData);
            for (var i = 0; i < searchCriteriaData.length; i++) {
                console.log("colName::" + "author." + searchCriteriaData[i].key + ":" + searchCriteriaData[i].value);
                var obj = {};
                obj[searchCriteriaData[i].colname] = searchCriteriaData[i].colvalue;
                conditionArray.push(obj);
            }
        }
        //  console.log("conditionArray::"+conditionArray);
        var User = mongoose.model('User');
        var Tweet = mongoose.model('Tweet');

        User.findOne({
            'username': req.user.username
        }, function(err, user) {
            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }

            if (!user) {
                console.log('No user with this username where found');
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }


            for (var i = 0; i < user.following.length; i++) {
                if (user.following[i].eventname) {
                    console.log(user.following[i].eventname);
                    idList.push(user.following[i].eventname);
                }
            }
            var criteria = (searchCriteriaData == 'undefined' || searchCriteriaData == '') ? '' : {
                $and: conditionArray
            };
            //console.log("criteria::"+{$and:conditionArray});

            /*Tweet.find(criteria).where('author.eventname').in(idList).sort({
              'timestamp': -1
            }).skip(req.params.skip).limit(req.params.limit).exec(function(err, tweets) {
			  if (err) {
                    console.log('There was an error while loading data from the database: ', err);
                    return res.json({
                        'error': 'Couldn\'t load tweets for the user dashboard'
                    }, 500);
                }

							for (var i = 0; i < tweets.length; i++) {
					console.log(tweets[i].timestamp);
				}

				res.json({
                    'tweets': tweets
                });
            });*/



				  console.log("display all records");
               Tweet.aggregate([{
                    '$match': {
                        'author.eventname': {
                            '$in': idList
                        }
                    }
                },
                {
                    "$sort": {
                        'timestamp': -1
                    }
                },

                {
                    "$skip": parseInt(req.params.skip)
                },
                {
                    "$limit": parseInt(req.params.limit)
                },

                {
                    "$lookup": {
                        "from": 'usertweetflags',
                        "localField": '_id',
                        "foreignField": 'tweet_id',
                        "as": 'tweetDetails'
                    }
                },
							   {
        '$addFields': {
            'tweetDetails': {
                '$arrayElemAt': [
                    {
                        '$filter': {
                            input: '$tweetDetails',
                            as: 'tweetDetail',
                            'cond': {
                               '$eq': [ '$$tweetDetail.username', req.session.username ]
							 }
                        }
                    }, 0
                ]
            }
        }
    }]).exec(function(err, tweets) {
		          console.log("flag::"+ req.params.displaytweet +" tweet length"+tweets.length);
			    	if(req.params.displaytweet=='flagtweet' && tweets.length>0  ){
					     for(var i = tweets.length -1; i >= 0 ; i--){
							// console.log("test::"+tweets[i].tweetDetails.tweet_flag);
                          if(tweets[i].tweetDetails===undefined){
							  tweets.splice(i, 1);
						  }
							 else if(typeof tweets[i].tweetDetails !== "undefined" && tweets[i].tweetDetails.tweet_flag=='N'){
							    tweets.splice(i, 1);
						   }else{

						   }
				    }
					}
			    if (err) {
                    console.log('There was an error while loading data from the database: ', err);
                    return res.json({
                        'error': 'Couldn\'t load tweets for the user dashboard'
                    }, 500);
                }
                res.json(   {
                             'tweets': tweets
                           }
				);
               });
		 });
    });

    app.post('/api/users/:username/tweets', auth.authenticate, function(req, res) {
        if (req.params.username !== req.user.username) {
            return res.json({
                'error': 'You are not allowed to tweet as another user'
            }, 403);
        }

        //TODO validation
        var Tweet = mongoose.model('Tweet');
        var tweet = new Tweet({
            'author': {
                'eventId': req.event.id,
                'eventname': req.event.eventname
            },
            'message': req.body.message
        });
        tweet.save(function(err) {
            if (err) {
                console.log('There was an error while storing the tweet to the database: ' + err);
                return res.json({
                    'error': 'Couldn\'t store the tweet'
                }, 500);
            } else {
                console.log('A new tweet was created with the id ' + tweet.id);
                return res.json({
                    'tweet': tweet
                });
            }
        });
    });


    app.post('/api/postComment/commentVal', function(req, res) {

        //TODO validation
        var Comment = mongoose.model('Comment');
        var comment = new Comment({
            'emp_id': req.body.emp_id,
            'commentVal': req.body.commentVal
        });
        comment.save(function(err) {
            if (err) {
                console.log('There was an error while storing the comment to the database: ' + err);
                return res.json({
                    'error': 'Couldn\'t store the tweet'
                }, 500);
            } else {
                console.log('A new comment was created with the id ' + comment.id);
                return res.json({
                    'comment': comment
                });
            }
        });
    });


    //// EVENT API
    app.get('/api/events', auth.authenticate, function(req, res) {
        var User = mongoose.model('User');
        var Event = mongoose.model('Event');

        var username = req.query.username;

        //if user is admin.so able to access restrcied status with restrict as well as normal.
        User.findOne({
            'username': username
        }, function(err, user) {
            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }

            if (!user) {
                console.log('No user with this username was not found2');
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }

            var followerMap = {};
            for (var i = 0; i < user.following.length; i++) {
                followerMap[user.following[i].eventname] = 1;
            }

            var userRole = username == "admin" ? {} : {
                'restricted': 'N'
            };
            console.log("userRole::" + JSON.stringify(userRole));
            //if user is admin,then able to access Normal as well as restricted  events.
            //if user is not admin or normal user,then access only normal events.
            Event.find(userRole, {
                'eventname': 1,
                'createdon': 1,
                'follow': 1,
                'parent_id':1,
                'parent_name':1,
                'event_message':1,
                'event_fields':1,
                'createdby': 1,
                'topic_definition':1,
                'topic_description':1
            }, {
                'sort': {
                    'eventname': 1,
                    'createdon': 1,
                    'follow': 1
                }
            }, function(err, events) {
                if (err) {
                    console.log('There was an error while loading data from the database: ', err);
                    return res.json({
                        'error': 'Couldn\'t load user'
                    }, 500);
                }

                for (var i = 0; i < events.length; i++) {
                    if (events[i].eventname in followerMap) {
                        events[i]['follow'] = true;
                    } else {
                        events[i]['follow'] = false;
                    }
                    events[i] = events[i].toObject({
                        virtuals: false
                    });
                }
                res.json({
                    'events': events
                });
            });
        });
    });

    app.post('/api/events/:eventname', auth.authenticate, function(req, res) {
        console.log("-----------------" + event);

        var Event = mongoose.model('Event');
        Event.findOne({
            'eventname': eventname
        }, function(err, event) {

            if (err) {
                console.log('There was an error while fetching event from database: ' + err);
                return res.json({
                    'error': 'Couldn\'t load event'
                }, 500);
            } else {
                console.log('event found ' + event.id);
                return res.json({
                    'event': event
                });
            }
        });
    });

    app.get('/api/events/followings', auth.authenticate, function(req, res) {

        var User = mongoose.model('User');
        var username = req.query.username;

        User.findOne({
            'username': username
        },
        function(err, user) {
            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }
            if (!user) {
                console.log('No user with this username was not found4');
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }

            return res.json({
                'user': user
            });
        });
    });

    app.put('/api/events/:eventname/follow', auth.authenticate, function(req, res) {

        var User = mongoose.model('User');

        User.findOne({
                'username': req.body.username
            }, {
                'id': 1,
                'username': 1,
                'name': 1
            },
            function(err, user) {
                if (err) {
                    console.log('There was an error while loading data from the database: ', err);
                    return res.json({
                        'error': 'Couldn\'t load user'
                    }, 500);
                }
                if (!user) {
                    console.log('No user with this username was not found3');
                    return res.json({
                        'error': 'Couldn\'t load user'
                    }, 500);
                }

                // User.find(
                //     {
                //         '_id': req.user.id
                // }, {
                //     following: {
                //         $elemMatch: {
                //             "eventname": req.body.following.parent_name
                //         }
                //     },
                //     function(err, numberAffected, raw) {

                //     if (err) {
                //         console.log('Couldn\'t save user');
                //         return res.json({
                //             'error': 'Couldn\'t save user'
                //         }, 500);
                //     }
                //     console.log('The user with id ' + req.user.id + ' follow now the event with id ' + following.parent_name);
                //     return res.json({
                //         'successful': true
                //     });
                //     }
                // });

                User.update({
                    '_id': req.user.id
                }, {
                    '$push': {
                        following: {
                            'eventId': req.body.following._id,
                            'eventname': req.body.following.parent_name
                        }
                    }
                }, function(err, numberAffected, raw) {
                    if (err) {
                        console.log('Couldn\'t save user');
                        return res.json({
                            'error': 'Couldn\'t save user'
                        }, 500);
                    }

                    //console.log('The user with id ' + req.user.id + ' follow now the event with id ');
                    return res.json({
                        'successful': true
                    });
                });
            });
    });
    var get_total_num_docs = function(db_client, query, cb) {
        db_client.collection(query['collection'], function(e, coll) {
            coll.find(query.params, query.options).count(function(e, count) {
                console.log(count);
                return cb(e, count);
            });
        });
    };
    app.put('/api/events/:eventname/unfollow', auth.authenticate, function(req, res) {
        var User = mongoose.model('User');
        var Event = mongoose.model('Event');

        User.findOne({
            'username': req.body.username
        }, {}, {
            'id': 1,
            'username': 1,
            'name': 1
        }, function(err, following) {

            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }
            if (!following) {
                console.log('No user with this username was not found4');
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }

            User.update({
                    '_id': req.user.id
                }, {
                    '$pull': {
                        following: {
                            'eventname': req.body.following.eventname,
                            'follow':req.body.following.follow
                        }
                    }
                },
                function(err, numberAffected, raw) {
                    if (err) {
                        console.log('Couldn\'t save user');
                        return res.json({
                            'error': 'Couldn\'t save user'
                        }, 500);
                    }

                    console.log('The user with id ' + req.user.id + ' unfollowed the event with id ' + following.id);
                    return res.json({
                        'successful': true
                    });
                });

            Event.update({
              '_id': req.body.following._id
            },{
                '$push': {
                    'follow': req.body.following.follow
                }
            }, function (err, result){
                if (err) {
                    console.log('Couldn\'t save user');
                    return res.json({
                        'error': 'Couldn\'t save user'
                    }, 500);
                }

                return res.json({
                    'successful': true
                });
            });

        });
    });

    app.post('/api/saverecentsearch', function(req, res) {
        console.log(req.body);
        var RecentSearch = mongoose.model('RecentSearch');
        RecentSearch.findOne({
            'emp_id': req.body.emp_id
        }, function(err, recentSearch) {
            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load tweet flag data'
                }, 500);
            }

			console.log("test::"+JSON.stringify(recentSearch));
            if (!recentSearch) {
                //new entry
                var currentDate = new Date();
                var recentSearch = new RecentSearch({
                    'emp_id': req.body.emp_id,
                    'description': req.body.description,
                    'date_time': req.body.date_time,
                    'sequence': req.body.sequence

                });

                recentSearch.save(function(err) {
                    if (err) {
                        console.log('There was an error while storing the flag tweet to the database: ' + err);
                        return res.json({
                            'error': 'Couldn\'t store the event'
                        }, 500);
                    } else {
                        return res.json({
                            'successful': true
                        });
                    }
                });
            } else {
                 RecentSearch.find({
                            'emp_id': req.body.emp_id
                        }, function(err, recentSearch) {
                            // $scope.descriptionArr = recentSearch[0].description;
                            if(recentSearch[0].description.length < 3) {
                            RecentSearch.update( { 'emp_id': req.body.emp_id },
                            { "$push": { "description":  req.body.description } },
                            function(err, numberAffected, raw) {
                                if (err) {
                                    console.log('Couldn\'t update userTweetFlag');
                                    return res.json({
                                        'error': 'Couldn\'t update userTweetFlag'
                                    }, 500);
                                }
                        return res.json({
                            'successful': true
                        });
                    });
                    }
                    else {
                        RecentSearch.update( { 'emp_id': req.body.emp_id },
                        { "$push": { "description":  req.body.description } },
                        function(err, numberAffected, raw) {
                            RecentSearch.update( { 'emp_id': req.body.emp_id },
                                { $pop: { description: -1 } },
                                function(err, numberAffected, raw) {
                                    if (err) {
                                        console.log('Couldn\'t update userTweetFlag');
                                        return res.json({
                                            'error': 'Couldn\'t update userTweetFlag'
                                        }, 500);
                                    }
                                    return res.json({
                                        'successful': true
                                    });
                                });
                            if (err) {
                                console.log('Couldn\'t update userTweetFlag');
                                return res.json({
                                    'error': 'Couldn\'t update userTweetFlag'
                                }, 500);
                            }
                    return res.json({
                        'recentSearch': recentSearch
                    });
                });
                    }
                });
            }
        });
    });

    app.get('/api/recentSearches', auth.authenticate, function(req, res) {
        var RecentSearch = mongoose.model('RecentSearch');
        RecentSearch.find({},
        function(err, recentSearcheData) {
            if (err) {
                console.log('There was an error while storing the flag tweet to the Database: ' + err);
                return res.json({
                    'error': 'Couldn\'t store the event'
                }, 500);
            }

            for(var i = 0 ;i < recentSearcheData.length; i++){
                if(recentSearcheData[i].emp_id === req.query.username){
                    recentSearcheData = recentSearcheData[i];
                }
            }

            return res.json({
                'res': recentSearcheData
            });
        });
    });

    /*get search List api */
    app.get('/api/searchList', auth.authenticate, function(req, res) {

        var User = mongoose.model('User');
        var Event = mongoose.model('Event');
        var username = req.query.username;

        //if user is admin.so able to access restrcied status with restrict as well as normal.
        User.findOne({
            'username': username
        }, function(err, user) {
            Event.aggregate([
                { $sort: { "createdon": -1} },
                { $group : {
                    "_id": "$parent_id",
                    "parent_name": {"$first": "$parent_name"},
                    search_list : { $push: "$$ROOT"}
                    }
                },
                { $project: {
                    parent_id : "$_id",
                    parent_name: 1,
                    search_list:1,
                    lastUpdated: {
                      $let: {
                        vars: {
                          firstUser: {
                            $arrayElemAt: ["$search_list", 0]
                          }
                        },
                        in: {
                          createdon: "$$firstUser.createdon"
                        }
                      }
                    }/*,
                     follow: {
                      $let: {
                        vars: {
                          firstUser: {
                            $arrayElemAt: ["$search_list", 0]
                          }
                        },
                        in: {
                          follow: "$$firstUser.follow"
                        }
                      }
                    }*/
                }}
            ], function(err, list){
                if (err) {
                    console.log('There was an error while storing the flag tweet to the database: ' + err);
                    return res.json({
                        'error': 'Couldn\'t store the event'
                    }, 500);
                }

                res.json({
                    'searchList': list
                });
            });
        })
    });

    /* Source system api Start */
    app.get('/api/source', auth.authenticate, function(req, res) {
        var User = mongoose.model('User');
        var Tweet = mongoose.model('Tweet');

        User.findOne({
            'username': username
        }, function(err, user) {
            Tweet.aggregate([
                {$group: {
                    '_id':"$author.sourceSystem",
                    count:{$sum:1}
                    }
                },
                { $project: {
                    'sourceSystem':"$_id",
                    count:1
                    }
                }
            ],function(err, source){
                if (err) {
                    console.log('There was an error while storing the flag tweet to the database: ' + err);
                    return res.json({
                        'error': 'Couldn\'t store the event'
                    }, 500);
                }

                res.json({
                    'source': source
                });
            });
        });
    })
    /* Source system api End */

    /* Get Subscribed List Api Start */
    app.get('/api/subscribedList', auth.authenticate, function(req, res) {

        var User = mongoose.model('User');
        var Event = mongoose.model('Event');
        var username = req.query.username;

        User.findOne({
            'username': username
        }, function(err, user) {
            Event.aggregate([
                { $sort: { "createdon": -1} },
                { $group : {
                    "_id": "$parent_id",
                    "parent_name": {"$first": "$parent_name"},
                    "follow": {"$first": "$follow"},
                    subscribed_list : { $push: "$$ROOT"}
                    }
                },
                { $project: {
                    parent_id : "$_id",
                    parent_name: 1,
                    follow:"$follow",
                    subscribed_list:1,
                    lastUpdated: {
                      $let: {
                        vars: {
                          firstUser: {
                            $arrayElemAt: ["$subscribed_list", 0]
                          }
                        },
                        in: {
                          createdon: "$$firstUser.createdon"
                        }
                      }
                    }
                }}
            ], function(err, subscribedList){
                if (err) {
                    console.log('There was an error while storing the flag tweet to the database: ' + err);
                    return res.json({
                        'error': 'Couldn\'t store the event'
                    }, 500);
                }

                res.json({
                    'subscribedList': subscribedList
                });
            });
        })
    });

    /*Get Subscribed List Api End */
    /* topic follow start*/
    app.put('/api/events/:eventname/topicFollow', auth.authenticate, function(req, res) {

        var User = mongoose.model('User');
        var Event = mongoose.model('Event');

        User.findOne({
                'username': req.body.username
            }, {
                'id': 1,
                'username': 1,
                'name': 1
            },
            function(err, following) {
                if (err) {
                    console.log('There was an error while loading data from the database: ', err);
                    return res.json({
                        'error': 'Couldn\'t load user'
                    }, 500);
                }

                Event.update(
                    {
                        '_id': req.body.following._id
                    },
                    {
                        $set: {
                            'follow': req.body.following.follow
                        }
                    }, function(err, event) {
                        if (err) {
                            console.log('Couldn\'t save user');
                            return res.json({
                                'error': 'Couldn\'t save user'
                            }, 500);
                        }
                        return res.json({
                            'successful': true
                    });
                })
            });
    });
    /*topic follow end*/

    /* topic unfollow start*/
    app.put('/api/events/:eventname/topicUnfollow', auth.authenticate, function(req, res) {
        var User = mongoose.model('User');
        var Event = mongoose.model('Event');

        User.findOne({
            'username': req.body.username
        }, {
            'id': 1,
            'username': 1,
            'name': 1
        }, function(err, following) {

            if (err) {
                console.log('There was an error while loading data from the database: ', err);
                return res.json({
                    'error': 'Couldn\'t load user'
                }, 500);
            }

            Event.update({
                    '_id': req.body.following._id
                },
                {
                    $set: {
                        'follow': req.body.following.follow
                    }
                }, function(err, event) {
                    if (err) {
                        console.log('Couldn\'t save user');
                        return res.json({
                            'error': 'Couldn\'t save user'
                        }, 500);
                    }
                    return res.json({
                        'successful': true
                });
            })

        });
    });
    /* topic unfollow end*/
};
