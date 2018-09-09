var url = require('url');
var expires = require('expires');
var bcrypt = require('bcrypt-nodejs');
var mongoose = require('mongoose');
var ldap = require('./ldap.js');


var TokenStorage = {};
var EXPIRES_AFTER = '1 hours';
var TOKEN_SALT = '';

function loadUserByUsername(username, callback) {
    var User = mongoose.model('User');
    User.findOne({
        'username': username
    }, function(err, user) {
        if (err) {
            return callback(err, null);
        }
        callback(null, user);
    });
}

function buildAuthToken(userRes, callback) {
    username = userRes.username;
    var expireTime = expires.after(EXPIRES_AFTER);
    generateToken(username, expireTime, function(err, authToken) {
        return callback(err, authToken, expireTime, userRes);
    });
}

function generateRawToken(username, expireTime) {
    return username + '+' + expireTime + '+' + TOKEN_SALT;
}

function generateToken(username, expireTime, callback) {
    bcrypt.hash(generateRawToken(username, expireTime), null, null, function(err, hash) {
        if (err) {
            return callback(err, null);
        }

        callback(null, hash);
    });
}

function checkAuthentication(req, res, callback) {
    if (!req.get('authorization')) {
        return res.json({
            'error': 'No authorization header send'
        }, 401);
    }

    var header = req.get('authorization') || '',
        token = header.split(/\s+/).pop() || '',
        auth = new Buffer(token, 'base64').toString(),
        parts = auth.split(/:/),
        username = parts[0],
        token = parts[1];
    expireTime = parts[2];

    if (!DEBUG && !(token in TokenStorage)) {
        return callback('Auth token is invalid', null);
    }

    if (expires.expired(expireTime)) {
        return res.json({
            'error': 'Auth token is expired'
        }, 401);
    }

    bcrypt.compare(generateRawToken(username, expireTime), token, function(err, res) {
        if (err) {
            callback(err, null);
        } else {
            if (res === true) {
                loadUserByUsername(username, function(err, user) {
                    if (err) {
                        return callback(err, null);
                    }
                    return callback(null, user, token);
                });
            } else {
                return callback('Auth token is invalid', null);
            }
        }
    });
}

function authenticate(req, res, next) {
    checkAuthentication(req, res, function(err, user, authToken) {
        if (err) {
            return res.json({
                'error': err
            }, 500);
        }

        req.user = user;
        req.authToken = authToken;
        next();
    });
}

function authenticateRoles(roles) {
    return function(req, res, next) {
        exports.authenticate(req, res, function() {
            next();
        });
    };
}

exports.init = function(app, Config) {
    if ('auth' in Config && 'expires' in Config.auth) {
        EXPIRES_AFTER = Config.auth.expires;
    }
    if ('auth' in Config && 'salt' in Config.auth) {
        TOKEN_SALT = Config.auth.salt;
    }

    app.post('/api/login', function(req, res) {


        var createTokenRes = function(err, authToken, expireTime, userRes) {
            if (err) {
                return res.json({
                    'error': 'Couldn\'t create auth token'
                }, 500);
            }
            req.authToken = authToken;
            if (!DEBUG) {
                TokenStorage[authToken] = true;
            }

            return res.json({
                'authToken': authToken,
                'expireTime': expireTime,
                'user': userRes
            });
        };


        try {
            var uiUserName = req.body.username;
            var uiUserPassword = req.body.password;
			
			//saving username to session.
			 req.session.username=req.body.username;
			 if ("admin" == uiUserName) {
              loadUserByUsername(uiUserName, function(err, dbUser) {
                   if (err || !dbUser || !(uiUserName==dbUser.username && uiUserPassword==dbUser.password )) {
                        return res.json({
                            'error': 'Username or password is invalid'
                        }, 401);
                    }
					  
                    console.log("welcome user::"+ dbUser.username);
                    //code to validate admin user password

                    var userRes = {
                        'name': dbUser.name,
                        'username': uiUserName,
                        'email': dbUser.mail,
                        'registered': dbUser.registered
                    }
                    buildAuthToken(userRes, createTokenRes);

                });


            } else {
                ldap.basicAuthMiddleware(req.body.username, req.body.password, function(err, userRes) {

                    if (err || !userRes) {
                        return res.json({
                            'error': 'Username or password is invalid'
                        }, 401);
                    }
                    req.user = userRes;

                    var User = mongoose.model('User');
                    User.find({
                        'email': userRes.email
                    }).exec(function(err, count) {
                        if (err) {
                            console.log('There was an error while counting existing users: ' + err);
                            return res.json({
                                'error': 'Couldn find password'
                            }, 500);
                        }
						
						 if (count < 1) {
							 
							
                            userRes.registered = new Date();
                            var userDbObject = new User(userRes);
                            userDbObject.save();
                        }
                    });

                    buildAuthToken(userRes, createTokenRes);

                });
            }
        } catch (e) {
            console.log("entering catch block");
            console.log(e);
            console.log("leaving catch block");
        }
    });



    app.get('/api/logout', authenticate, function(req, res) {
		console.log("Wihtin auth");
		//destroying session object.
		req.session.destroy();
		  if (!DEBUG) {
			 delete TokenStorage[req.authToken];
        }
	   return res.json({
            'loggedout': true
        });
    });

};

exports.authenticate = authenticate;
exports.authenticateRoles = authenticateRoles;