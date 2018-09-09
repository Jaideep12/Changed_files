var LdapAuth = require('ldapauth-fork');

module.exports = {

	basicAuthMiddleware : function(username, password,callback){
	var options ={
		url: 'ldap://192.168.1.16:389/',
		bindDn: 'ezest\\' + username,
		bindCredentials: password,
		searchBase: 'DC=EZEST,DC=Local',
		searchFilter: "(sAMAccountName={{username}})"
	};

	var ldap = new LdapAuth(options);
	ldap.authenticate(username,password, function(err, user) {
		if(err) {
			return callback(err, null);
		}
		
		var userResponse ={
			'name' : user.name,
			'username' : user.sAMAccountName,
			'displayName' : user.displayName,
			'email' : user.mail,
			'surname' : user.sn,
			'registered': user.registered
		}
		return callback(null, userResponse); 
	});
}
}