var express = require('express');  
var app = express();
var Twit = require('twit');
var mysql = require('mysql');

function ajouter_slash(chaine){
	chaine = chaine.replace(/\\/g,"\\\\");
	chaine = chaine.replace(/\'/g,"\\'");
	chaine = chaine.replace(/\"/g,"\\\"");
	chaine = encodeURIComponent(chaine);
	
	return chaine;
};

function StringToDate(s) {
  var b = s.split(/[: ]/g);
  var m = {jan:0, feb:1, mar:2, apr:3, may:4, jun:5, jul:6,
           aug:7, sep:8, oct:9, nov:10, dec:11};

  return new Date(Date.UTC(b[7], m[b[1].toLowerCase()], b[2], b[3], b[4], b[5]));
}

//Configuration MySQL
var connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : 'root'
});

//Connexion MySQL et indication erreur
connection.connect(function(err) {
  if (err) {
    console.error('erreur de connexion: ' + err.stack);
    return;
  }

  console.log('connecté avec id ' + connection.threadId);
});

//Création de la base de données SQL si elle n'existe pas
connection.query('CREATE DATABASE IF NOT EXISTS geo_twitter', function(err, results) {
		if (err) {
			console.log("ERREUR: " + err.message);
			throw err;
		}
});

//Indication d'utilisation de la base geo_twitter
connection.query('USE geo_twitter', function(err, results) {
		if (err) {
			console.log("ERREUR: " + err.message);
			throw err;
		}
});

//Création de la table de listing des tweets
connection.query('CREATE TABLE IF NOT EXISTS listing_tweet'+
				 '(id INT(11) NOT NULL AUTO_INCREMENT, '+
				 'tweets TEXT,'+
				 'user_id BIGINT,'+
				 'Compte_protect TINYTEXT,'+
				 'latitude_i DOUBLE,'+
				 'longitude_i DOUBLE,'+
				 'date_creation_tweet BIGINT,'+
				 'tweet_id BIGINT,'+
				 'PRIMARY KEY (id));'
);

//Création de la table de listing des retweets
connection.query('CREATE TABLE IF NOT EXISTS listing_retweet'+
				 '(id INT(11) NOT NULL AUTO_INCREMENT, '+
				 'tweets TEXT,'+
				 'retweet_user_id BIGINT,'+
				 'first_user_id BIGINT,'+
				 'Compte_protect TINYTEXT,'+
				 'latitude_i DOUBLE,'+
				 'longitude_i DOUBLE,'+
				 'date_creation_tweet BIGINT,'+
				 'tweet_id BIGINT,'+
				 'PRIMARY KEY (id));'
);

//Création de la table des Hashtags mentionnés dans les tweets
connection.query('CREATE TABLE IF NOT EXISTS listing_hashtags'+
				 '(id INT(11) NOT NULL AUTO_INCREMENT,'+
				 'tweet_id BIGINT,'+
				 'hashtags TINYTEXT,'+
				 'PRIMARY KEY (id));'
);

//Création de la table pour les utilisateurs mentionnés dans les tweets
connection.query('CREATE TABLE IF NOT EXISTS listing_utilisateurs'+
				 '(id INT(11) NOT NULL AUTO_INCREMENT,'+
				 'tweet_id BIGINT,'+
				 'util_mention TINYTEXT,'+
				 'PRIMARY KEY (id));'
);

//Configuration pour connexion stream twitter
var T = new Twit({  
    consumer_key        : 'YOUR',
    consumer_secret     : 'YOUR',
    access_token        : 'YOUR',
    access_token_secret : 'YOUR'
});

//Coordonnées sud-ouest et nord-est de la france
var france = ['-6.9','40.8','9.9','51.7']

//Indication de filtrage des tweets du stream
var stream = T.stream('statuses/filter', { locations: france , language: 'fr'});

//var stream = T.stream('statuses/sample');

//Récupération des tweets et stockage des informations
stream.on('tweet', function (tweet) {
    var msg = {};
	var UtilMentionId = [];
	var UtilMentionName=[];
	var msg_retweet = [];
	var HashTags = [];
	var latitude;
	var longitude;
	var latitude_first_tweet;
	var longitude_first_tweet;
	
	date_creation = tweet.created_at;
	date_en_chiffre = Number(StringToDate(date_creation));
	
	id_du_tweet = tweet.id;
	
	msg.text = ajouter_slash(tweet.text);
	
	if(tweet.coordinates){
		latitude = tweet.coordinates.coordinates[1];
		longitude = tweet.coordinates.coordinates[0];
	}
		
	msg.user = {
        name: tweet.user.name,
        image: tweet.user.profile_image_url,
		compte_protege: tweet.user.protected,
		id_user: tweet.user.id
    };
	
	msg.hashtags = tweet.hashtags;
	msg.UtilMention = tweet.entities;
	
	//Enregistrement dans une variable les informations du first tweet s'il s'agit d'un retweet
	msg_retweet.push(tweet.retweeted_status);
	
	//S'il s'agit d'un retweet alors enregistrement des informations
	if(msg_retweet[0]){		
		if(msg_retweet[0].coordinates){
			latitude_first_tweet = msg_retweet[0].coordinates.coordinates[1];
			longitude_first_tweet = msg_retweet[0].coordinates.coordinates[0];
		}
		
		//Insertion des informations du first tweet et de l'utilisateur qui a retweet dans la table
		connection.query('INSERT INTO listing_retweet'+
						 ' SET tweets = ?'+
						 ', retweet_user_id = ?'+
						 ', first_user_id = ?'+
						 ', compte_protect = ?'+
						 ', latitude_i = ?'+
						 ', longitude_i = ?'+
						 ', date_creation_tweet = ?'+
						 ', tweet_id = ?',
						 [msg.text,	msg.user.id_user, msg_retweet[0].user.id, msg_retweet[0].user.compte_protege, latitude_first_tweet, longitude_first_tweet, Number(StringToDate(msg_retweet[0].created_at)), msg_retweet[0].id],
							 function(err, results) {
								if (err) {
									console.log("ERREUR Table Retweet: " + err.message);
								}
							 }
						 );
		
	} else {
	
		//Insertion des informations du tweet dans la table
		connection.query('INSERT INTO listing_tweet'+
						 ' SET tweets = ?'+
						 ', user_id = ?'+
						 ', compte_protect = ?'+
						 ', latitude_i = ?'+
						 ', longitude_i = ?'+
						 ', date_creation_tweet = ?'+
						 ', tweet_id = ?',
						 [msg.text,	msg.user.id_user, msg.user.compte_protege, latitude, longitude, date_en_chiffre, id_du_tweet],
							 function(err, results) {
								if (err) {
									console.log("ERREUR Table Tweet: " + err.message);
								}
							 }
						 );
		
		for(var i=0;i<15;i++){
			if(msg.UtilMention.user_mentions[i]){
				UtilMentionName.push(msg.UtilMention.user_mentions[i].name);
				UtilMentionId.push(msg.UtilMention.user_mentions[i].id);
				//Insertion des utilisateurs mentionnés dans les tweets dans une table
				connection.query('INSERT INTO listing_utilisateurs'+
								 ' SET tweet_id = ?'+
								 ', util_mention = ?',
								 [id_du_tweet, msg.UtilMention.user_mentions[i].name],
									 function(err, results) {
										if (err) {
											console.log("ERREUR Table Utilisateur: " + err.message);
										}
									 }
								 
								 );
			};
		}
		
		for(var i=0;i<15;i++){
			if(msg.UtilMention.hashtags[i]){
				HashTags.push(msg.UtilMention.hashtags[i].text);
				//Insertion des hashtags mentionnés dans les tweets dans une table
				connection.query('INSERT INTO listing_hashtags'+
								 ' SET tweet_id = ?'+
								 ', hashtags = ?',
								 [id_du_tweet, msg.UtilMention.hashtags[i].text],
									 function(err, results) {
										if (err) {
											console.log("ERREUR Table HashTags: " + err.message);
										}
									 }
								 );
			};
		}
	}
});