
// A local cache lookup
var localCache = [];


/**
  Lookup a username and return whether it is exists or not.

  @function lookup
  @param {String} username to look up
  @return {Promise} promise that results to whether the name was found or not
**/
function lookup(username) {
  return new Em.RSVP.Promise(function (resolve) {

    if (username.length < Discourse.SiteSettings.min_username_length) {
      resolve(false);
      return;
    }

    var cached = localCache[username];

    // If we have a cached answer, return it
    if (typeof cached !== "undefined") {
      if (cached.then) {
        cached.then(function(username){resolve(username)});
      } else {
        resolve(cached);
      }
    } else {
      Discourse.ajax("/users/is_local_username", { data: { username: username } }).then(function(r) {
        // lower case i turkish bug?
        localCache[username] = _.include(r.valid, username.toLowerCase);
        resolve(r.valid);
      });
    }
  });
}

/**
  Help us link directly to a mentioned user's profile if the username exists.

  @class Mention
  @namespace Discourse
  @module Discourse
**/
Discourse.Mention = {

  /**
    Paints an element in the DOM with the appropriate classes and markup if the username
    it is mentioning exists.

    @method paint
    @param {Element} the element in the DOM to decorate
  **/
  paint: function(e) {
    var $elem = $(e);
    if ($elem.data('mention-tested')) return;
    var username = $elem.text().substr(1);

    $elem.addClass('mention-loading');
    lookup(username).then(function(found) {
      if (found) {
        $elem.replaceWith("<a href='" + Discourse.getURL("/users/") + username.toLowerCase() + "' class='mention'>@" + username + "</a>");
      } else {
        $elem.removeClass('mention-loading').addClass('mention-tested');
      }
    });
  }
};
