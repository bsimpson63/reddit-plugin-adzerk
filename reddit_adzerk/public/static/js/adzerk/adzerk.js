!(function(global, $, undefined) {
  'use strict';

  global.r = global.r || {};

  r.adzerk = {

    createSponsorshipAdFrame: function(overrideSrc) {
      var $iframe = $('<iframe>');

      $iframe
        .attr({
            id: 'ad_sponsorship',
            src: '//' + r.config.media_domain + '/ads/display/300x250-companion',
            frameBorder: 0,
            scrolling: 'no',
        });

      $('.side .sponsorshipbox')
        .empty()
        .append($iframe);
    },

  };

  $(global).on('message', function(e) {
    e = e.originalEvent;

    if (!new RegExp('^http(s)?:\\/\\/' + r.config.media_domain, 'i').test(e.origin)) {
      return;
    }

    var message = e.data.split(':');

    if (message[0] == 'ados.createAdFrame') {
      r.adzerk.createSponsorshipAdFrame();
    }
  });

})(this, this.jQuery);
